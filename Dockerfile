FROM golang:1.13

WORKDIR /app

COPY . .
RUN mv docs-example docs

ARG MAILROOM_PARSE_SERVER_APP_ID
ENV MAILROOM_PARSE_SERVER_APP_ID=$MAILROOM_PARSE_SERVER_APP_ID

ARG MAILROOM_PARSE_SERVER_MASTER_KEY
ENV MAILROOM_PARSE_SERVER_MASTER_KEY=$MAILROOM_PARSE_SERVER_MASTER_KEY

ARG MAILROOM_PARSE_SERVER_URL
ENV MAILROOM_PARSE_SERVER_URL=$MAILROOM_PARSE_SERVER_URL

ARG MAILROOM_YOURLS_LOGIN
ENV MAILROOM_YOURLS_LOGIN=$MAILROOM_YOURLS_LOGIN

ARG MAILROOM_YOURLS_PASSWORD
ENV MAILROOM_YOURLS_PASSWORD=$MAILROOM_YOURLS_PASSWORD

ARG MAILROOM_YOURLS_HOST
ENV MAILROOM_YOURLS_HOST=$MAILROOM_YOURLS_HOST

ARG MAILROOM_SHORTEN_URL_PING
ENV MAILROOM_SHORTEN_URL_PING=$MAILROOM_SHORTEN_URL_PING

ARG MAILROOM_DOMAIN
ENV MAILROOM_DOMAIN=$MAILROOM_DOMAIN

ARG MAILROOM_SPELL_CHECKER_KEY
ENV MAILROOM_SPELL_CHECKER_KEY=$MAILROOM_SPELL_CHECKER_KEY

RUN go build ./cmd/mailroom && chmod +x mailroom

EXPOSE 80
ENTRYPOINT ["./mailroom"]