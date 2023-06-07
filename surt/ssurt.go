/*
 * Copyright 2020 National Library of Norway.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package surt

import (
	"net"
	"strings"

	"github.com/nlnwa/whatwg-url/url"
)

func UrlToSsurtHostname(uri string) (string, error) {
	u, err := parser.Parse(uri)
	if err != nil {
		return "", err
	}
	sb := new(strings.Builder)
	writeHostName(sb, u)
	return sb.String(), nil
}

func UrlToSsurt(u *url.Url) string {
	// TODO normalize search params, e.g. remove session tokens
	u.SearchParams().Sort()

	sb := new(strings.Builder)

	writeHostName(sb, u)
	writeScheme(sb, u)
	sb.WriteString(u.Pathname())
	sb.WriteString(u.Search())
	sb.WriteString(u.Hash())

	return sb.String()
}

func StringToSsurt(uri string) (string, error) {
	u, err := parser.Parse(uri)
	if err != nil {
		return "", err
	}
	return UrlToSsurt(u), nil
}

func writeHostName(sb *strings.Builder, u *url.Url) {
	hostname := u.Hostname()
	if hostname == "" {
		return
	}
	if hostname[0] == '[' {
		sb.WriteString(hostname)
	} else if net.ParseIP(hostname).To4() != nil {
		sb.WriteString(hostname)
	} else {
		t := strings.Split(hostname, ".")
		for i := len(t) - 1; i >= 0; i-- {
			sb.WriteString(t[i])
			sb.WriteByte(',')
		}
	}

	sb.WriteString("//")
}

func writeScheme(sb *strings.Builder, u *url.Url) {
	if u.Port() != "" {
		sb.WriteString(u.Port())
		sb.WriteByte(':')
	}
	scheme := u.Scheme()
	sb.WriteString(scheme)
	if u.Username() != "" {
		sb.WriteByte('@')
		sb.WriteString(u.Username())
	}
	if u.Password() != "" {
		sb.WriteByte(':')
		sb.WriteString(u.Password())
	}
	sb.WriteByte(':')
}
