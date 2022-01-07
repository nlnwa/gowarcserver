/*
 * Copyright 2021 National Library of Norway.
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

package proxy

import (
	log "github.com/sirupsen/logrus"
	"net/url"
)

func ParseUrls(urlStrs []string) []*url.URL {
	var childUrls []*url.URL
	for _, urlstr := range urlStrs {
		if u, err := url.Parse(urlstr); err != nil {
			log.Warnf("Parsing config child url %s failed with error %v", urlstr, err)
		} else {
			childUrls = append(childUrls, u)
		}
	}
	return childUrls
}
