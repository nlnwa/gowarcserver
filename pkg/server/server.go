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

package server

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/nlnwa/gowarcserver/pkg/index"
	"github.com/nlnwa/gowarcserver/pkg/loader"
	"github.com/nlnwa/gowarcserver/pkg/server/warcserver"
)

func Serve(db *index.Db, port int) error {
	l := &loader.Loader{
		Resolver: &storageRefResolver{db: db},
		Loader: &loader.FileStorageLoader{FilePathResolver: func(fileName string) (filePath string, err error) {
			fileInfo, err := db.GetFilePath(fileName)
			return fileInfo.Path, err
		}},
		NoUnpack: false,
	}

	r := mux.NewRouter()
	r.Handle("/id/{id}", &contentHandler{l})
	r.Handle("/files/", &fileHandler{l, db})
	r.Handle("/search", &searchHandler{l, db})
	warcserverRoutes := r.PathPrefix("/warcserver").Subrouter()
	warcserver.RegisterRoutes(warcserverRoutes, db, l)
	http.Handle("/", r)

	loggingMw := func(h http.Handler) http.Handler {
		return handlers.CombinedLoggingHandler(os.Stdout, h)
	}
	r.Use(loggingMw)

	portStr := strconv.Itoa(port)
	httpServer := &http.Server{
		Addr: fmt.Sprintf(":%v", portStr),
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigs
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		httpServer.Shutdown(ctx)
	}()

	return httpServer.ListenAndServe()
}

type storageRefResolver struct {
	db *index.Db
}

func (m *storageRefResolver) Resolve(warcId string) (storageRef string, err error) {
	return m.db.GetStorageRef(warcId)
}
