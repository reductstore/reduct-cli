// Copyright 2026 ReductStore
// This Source Code Form is subject to the terms of the Mozilla Public
//    License, v. 2.0. If a copy of the MPL was not distributed with this
//    file, You can obtain one at https://mozilla.org/MPL/2.0/.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::{anyhow, bail, Context};
use clap::{Arg, ArgMatches, Command};
use http_body_util::Full;
use hyper::{
    body::{Bytes, Incoming},
    header::{CONNECTION, CONTENT_TYPE},
    server::conn::http1,
    service::service_fn,
    Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use tokio::{
    net::TcpListener,
    sync::{oneshot, Mutex},
    time::timeout,
};
use url::Url;

use crate::{
    cmd::ALIAS_OR_URL_HELP,
    config::{find_alias, ConfigFile},
    context::CliContext,
    io::std::output,
};

const LOGIN_TIMEOUT: Duration = Duration::from_secs(300);

pub(crate) fn login_cmd() -> Command {
    Command::new("login")
        .about("Authenticate an alias or URL through OAuth2")
        .arg(
            Arg::new("ALIAS_OR_URL")
                .help(ALIAS_OR_URL_HELP)
                .required(true),
        )
}

pub(crate) async fn login_handler(ctx: &CliContext, args: &ArgMatches) -> anyhow::Result<()> {
    let alias_or_url = args
        .get_one::<String>("ALIAS_OR_URL")
        .expect("required argument");
    let target = login_target(ctx, alias_or_url)?;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .context("Failed to start local OAuth2 callback server")?;
    let login_url = login_url(&target.url, listener.local_addr()?)?;

    output!(ctx, "Open this URL to authenticate:\n  {login_url}");
    let callback = timeout(LOGIN_TIMEOUT, receive_callback(&listener))
        .await
        .map_err(|_| anyhow!("Timed out waiting for OAuth2 authentication callback"))??;
    if let Some(alias_name) = target.alias_name {
        save_token(ctx, &alias_name, &callback.access_token)?;
        output!(
            ctx,
            "Logged in to '{}'. Token expires in {}s.",
            alias_name,
            callback.expires_in
        );
    } else {
        let mut authenticated_url = target.url;
        authenticated_url
            .set_username(&callback.access_token)
            .expect("gateway URL supports a username");
        output!(
            ctx,
            "Logged in to '{}'. Token expires in {}s.\nUse this URL for future commands:\n  {}",
            target.name,
            callback.expires_in,
            authenticated_url
        );
    }
    Ok(())
}

struct LoginTarget {
    name: String,
    url: Url,
    alias_name: Option<String>,
}

struct Callback {
    access_token: String,
    expires_in: u64,
}

fn login_target(ctx: &CliContext, alias_or_url: &str) -> anyhow::Result<LoginTarget> {
    if let Ok(alias) = find_alias(ctx, alias_or_url) {
        return Ok(LoginTarget {
            name: alias_or_url.to_owned(),
            url: alias.url,
            alias_name: Some(alias_or_url.to_owned()),
        });
    }

    let mut url = Url::parse(alias_or_url)
        .map_err(|_| anyhow!("'{}' isn't an alias or a valid HTTP URL", alias_or_url))?;
    if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
        bail!("'{}' isn't an alias or a valid HTTP URL", alias_or_url);
    }
    url.set_username("")
        .map_err(|_| anyhow!("'{}' isn't an alias or a valid HTTP URL", alias_or_url))?;
    url.set_password(None)
        .map_err(|_| anyhow!("'{}' isn't an alias or a valid HTTP URL", alias_or_url))?;
    url.set_query(None);
    url.set_fragment(None);
    Ok(LoginTarget {
        name: url.to_string(),
        url,
        alias_name: None,
    })
}

fn login_url(gateway_url: &Url, callback_addr: SocketAddr) -> anyhow::Result<Url> {
    let mut url = gateway_url.join("/api/v1/auth/login")?;
    let callback_url = format!("http://{callback_addr}/callback");
    url.query_pairs_mut()
        .append_pair("cli_redirect", &callback_url);
    Ok(url)
}

async fn receive_callback(listener: &TcpListener) -> anyhow::Result<Callback> {
    let (stream, _) = listener.accept().await?;
    let (sender, receiver) = oneshot::channel();
    let sender = Arc::new(Mutex::new(Some(sender)));

    http1::Builder::new()
        .serve_connection(
            TokioIo::new(stream),
            service_fn(move |request| handle_callback(request, Arc::clone(&sender))),
        )
        .await
        .context("Failed to receive OAuth2 callback")?;

    receiver
        .await
        .context("OAuth2 callback closed without a response")?
}

async fn handle_callback(
    request: Request<Incoming>,
    sender: Arc<Mutex<Option<oneshot::Sender<anyhow::Result<Callback>>>>>,
) -> Result<Response<Full<Bytes>>, hyper::Error> {
    let callback = if request.method() == Method::GET {
        let request_target = request
            .uri()
            .path_and_query()
            .map_or("/", |path_and_query| path_and_query.as_str());
        parse_callback(request_target)
    } else {
        Err(anyhow!("OAuth2 callback must use GET"))
    };
    let response = callback_response(&callback);

    if let Some(sender) = sender.lock().await.take() {
        let _ = sender.send(callback);
    }

    Ok(response)
}

fn callback_response(callback: &anyhow::Result<Callback>) -> Response<Full<Bytes>> {
    let mut response = Response::builder().header(CONNECTION, "close");
    if callback.is_ok() {
        response = response
            .status(StatusCode::OK)
            .header(CONTENT_TYPE, "text/html; charset=utf-8");
        response.body(Full::new(Bytes::from_static(
            b"<html><body>Authentication successful. You can close this tab.</body></html>",
        )))
    } else {
        response
            .status(StatusCode::BAD_REQUEST)
            .body(Full::new(Bytes::new()))
    }
    .expect("valid OAuth2 callback response")
}

fn parse_callback(request_target: &str) -> anyhow::Result<Callback> {
    let url = Url::parse(&format!("http://localhost{request_target}"))?;
    if url.path() != "/callback" {
        bail!("OAuth2 callback has an invalid path");
    }
    let query = url
        .query_pairs()
        .collect::<std::collections::HashMap<_, _>>();
    let access_token = query
        .get("access_token")
        .filter(|token| !token.is_empty())
        .context("OAuth2 callback is missing access token")?
        .to_string();
    let expires_in = query
        .get("expires_in")
        .context("OAuth2 callback is missing token expiration")?
        .parse()
        .context("OAuth2 callback has invalid token expiration")?;
    Ok(Callback {
        access_token,
        expires_in,
    })
}

fn save_token(ctx: &CliContext, alias_name: &str, access_token: &str) -> anyhow::Result<()> {
    let mut config_file = ConfigFile::load(ctx.config_path())?;
    let alias = config_file
        .mut_config()
        .aliases
        .get_mut(alias_name)
        .ok_or_else(|| anyhow!("Alias '{}' does not exist", alias_name))?;
    alias.token = access_token.to_owned();
    config_file.save()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::ConfigFile, context::tests::context};
    use rstest::rstest;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    };

    #[test]
    fn parses_callback_token_and_expiration() {
        let callback =
            parse_callback("/callback?access_token=token%20value&expires_in=3600").unwrap();
        assert_eq!(callback.access_token, "token value");
        assert_eq!(callback.expires_in, 3600);
    }

    #[test]
    fn rejects_invalid_callback() {
        assert!(parse_callback("/other?access_token=token&expires_in=3600").is_err());
        assert!(parse_callback("/callback?expires_in=3600").is_err());
        assert!(parse_callback("/callback?access_token=token&expires_in=invalid").is_err());
    }

    #[rstest]
    fn saves_received_token(context: CliContext) {
        save_token(&context, "default", "oauth-token").unwrap();
        let config = ConfigFile::load(context.config_path()).unwrap();
        assert_eq!(config.config().aliases["default"].token, "oauth-token");
    }

    #[test]
    fn constructs_loopback_login_url() {
        let url = login_url(
            &Url::parse("https://gateway.example.com").unwrap(),
            "127.0.0.1:1234".parse().unwrap(),
        )
        .unwrap();
        assert_eq!(url.as_str(), "https://gateway.example.com/api/v1/auth/login?cli_redirect=http%3A%2F%2F127.0.0.1%3A1234%2Fcallback");
    }

    #[rstest]
    fn resolves_direct_gateway_url(context: CliContext) {
        let target =
            login_target(&context, "http://old-token@localhost:8384?ignored=value").unwrap();
        assert_eq!(target.name, "http://localhost:8384/");
        assert_eq!(target.url.as_str(), "http://localhost:8384/");
        assert_eq!(target.alias_name, None);
    }

    #[rstest]
    fn rejects_invalid_login_target(context: CliContext) {
        assert!(login_target(&context, "invalid-target").is_err());
        assert!(login_target(&context, "ftp://localhost:8384").is_err());
    }

    #[tokio::test]
    async fn receives_http_callback() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let callback = tokio::spawn(async move { receive_callback(&listener).await });
        let mut stream = TcpStream::connect(address).await.unwrap();
        stream
            .write_all(
                b"GET /callback?access_token=oauth-token&expires_in=3600 HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();

        let callback = callback.await.unwrap().unwrap();
        assert_eq!(callback.access_token, "oauth-token");
        assert_eq!(callback.expires_in, 3600);
        assert!(std::str::from_utf8(&response)
            .unwrap()
            .contains("Authentication successful"));
    }
}
