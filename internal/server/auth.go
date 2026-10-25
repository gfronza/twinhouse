package server

import (
	"crypto/subtle"
	"encoding/base64"
	"net/http"
	"net/url"
	"strings"

	"dynamodb-eventstream/internal/cherr"
)

const defaultUser = "default"

// credentials extracts the user and password the way ClickHouse's HTTP
// handler does. Exactly one source may be used: X-ClickHouse-User/Key
// headers, the Authorization header (Basic only), or user/password URL
// parameters. Without any, the user is "default" with an empty password.
func credentials(r *http.Request, q url.Values) (user, password string, err error) {
	hUser, hasHUser := headerValue(r.Header, "X-ClickHouse-User")
	hKey, hasHKey := headerValue(r.Header, "X-ClickHouse-Key")
	authz, hasAuthz := headerValue(r.Header, "Authorization")
	pUser, hasPUser := queryValue(q, "user")
	pPass, hasPPass := queryValue(q, "password")

	hasHeaders := hasHUser || hasHKey
	hasParams := hasPUser || hasPPass
	switch {
	case hasHeaders && hasAuthz:
		return "", "", authError("Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and Authorization HTTP header simultaneously")
	case hasHeaders && hasParams:
		return "", "", authError("Invalid authentication: it is not allowed to use X-ClickHouse HTTP headers and authentication via parameters simultaneously")
	case hasAuthz && hasParams:
		return "", "", authError("Invalid authentication: it is not allowed to use Authorization HTTP header and authentication via parameters simultaneously")
	}

	switch {
	case hasHeaders:
		if hUser == "" {
			return "", "", authError("Got an empty user name from X-ClickHouse HTTP headers")
		}
		return hUser, hKey, nil
	case hasAuthz:
		scheme, rest, _ := strings.Cut(strings.TrimSpace(authz), " ")
		if !strings.EqualFold(scheme, "Basic") {
			return "", "", authError("Invalid authentication: '%s' HTTP Authorization scheme is not supported", scheme)
		}
		raw, decErr := base64.StdEncoding.DecodeString(strings.TrimSpace(rest))
		u, p, _ := strings.Cut(string(raw), ":")
		if decErr != nil || u == "" {
			return "", "", authError("Got an empty user name from Authorization HTTP header")
		}
		return u, p, nil
	case hasPUser:
		if pUser == "" {
			return "", "", authError("Got an empty user name from authentication via parameters")
		}
		return pUser, pPass, nil
	}
	return defaultUser, pPass, nil
}

// authenticate checks user/password against the configured users.
// ClickHouse answers a failed login of the default user with
// REQUIRED_PASSWORD (401) and every other failure with AUTHENTICATION_FAILED (403).
func authenticate(users map[string]string, user, password string) error {
	want, ok := users[user]
	if ok && subtle.ConstantTimeCompare([]byte(want), []byte(password)) == 1 {
		return nil
	}
	if user == defaultUser {
		return cherr.New(cherr.RequiredPassword,
			"%s: Authentication failed: password is incorrect, or there is no user with such name.\n\n"+
				"The users and passwords of this server are configured with the CH_USERS environment variable.\n\n", user)
	}
	return authError("%s: Authentication failed: password is incorrect, or there is no user with such name", user)
}

func authError(format string, args ...any) *cherr.Error {
	return cherr.New(cherr.AuthenticationFailed, format, args...)
}

func headerValue(h http.Header, name string) (string, bool) {
	v, ok := h[http.CanonicalHeaderKey(name)]
	if !ok || len(v) == 0 {
		return "", false
	}
	return v[0], true
}

func queryValue(q url.Values, name string) (string, bool) {
	v, ok := q[name]
	if !ok || len(v) == 0 {
		return "", false
	}
	return v[0], true
}
