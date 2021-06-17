package handlers

import "net/url"

func BuildChildURLString(base *url.URL, v *url.URL) *url.URL {
	u := *base

	u.Path = v.Path
	u.RawPath = v.RawPath
	u.RawQuery = v.RawQuery
	u.Fragment = v.Fragment

	return &u
}
