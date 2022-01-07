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

func SsurtHostname(uri string) (string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", err
	}
	result := new(strings.Builder)
	writeHostName(u, result)
	return result.String(), nil
}

func SsurtUrl(u *url.Url, includeScheme bool) (string, error) {
	u.SearchParams().Sort()

	result := new(strings.Builder)
	writeHostName(u, result)

	if includeScheme {
		writeScheme(u, result)
	}
	result.WriteString(u.Pathname())
	result.WriteString(u.Search())
	result.WriteString(u.Hash())

	return result.String(), nil
}

func SsurtString(u string, includeScheme bool) (string, error) {
	u2, err := url.Parse(u)
	if err != nil {
		return "", err
	}
	return SsurtUrl(u2, includeScheme)
}

func writeHostName(u *url.Url, sb *strings.Builder) {
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

func writeScheme(u *url.Url, sb *strings.Builder) {
	if u.Port() != "" {
		sb.WriteString(u.Port())
		sb.WriteByte(':')
	}
	sb.WriteString(strings.TrimSuffix(u.Protocol(), ":"))
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
