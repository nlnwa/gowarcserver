package index

import (
	"github.com/fsnotify/fsnotify"
	"github.com/rs/zerolog/log"
	"os"
)

func newWatcher() (*watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	return &watcher{
		Watcher: w,
	}, nil
}

type watcher struct {
	*fsnotify.Watcher
}

func (w *watcher) Close() {
	if w == nil {
		return
	}
	_ = w.Watcher.Close()
}

func (w *watcher) Add(dir string) error {
	if w == nil {
		return nil
	}
	return w.Watcher.Add(dir)
}

func (w *watcher) Watch(perFileFn func(string)) {
	for {
		select {
		case event, ok := <-w.Events:
			if !ok {
				return
			}
			if event.Op&fsnotify.Write == fsnotify.Write {
				perFileFn(event.Name)
			} else if event.Op&fsnotify.Create == fsnotify.Create {
				if info, err := os.Stat(event.Name); err != nil {
					log.Warn().Msgf("Watcher failed to stat new file event: %v: %v", event.Name, err)
					continue
				} else if !info.Mode().IsDir() {
					continue
				}

				if err := w.Add(event.Name); err != nil {
					log.Warn().Msgf("Watcher failed to add new directory: %s, %v", event.Name, err)
				}
			}
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			log.Warn().Msgf("Watcher error: %v", err)
		}
	}
}
