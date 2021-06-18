package localhttp

import "net/url"

func BuildChildURLString(childBase *url.URL, nodeUrl *url.URL) string {
	rtr := url.URL{}
	rtr.Scheme = childBase.Scheme
	rtr.Host = childBase.Host
	rtr.Path = nodeUrl.Path
	rtr.RawQuery = nodeUrl.RawQuery
	rtr.Fragment = nodeUrl.Fragment

	return rtr.String()
}
