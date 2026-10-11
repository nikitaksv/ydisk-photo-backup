package main

import (
	"fmt"
	"golang.org/x/sys/windows"
	"os"
)

func lockRoot(root string) (func(), error) {
	f, e := os.OpenFile(root+string(os.PathSeparator)+"backup.lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	overlapped := &windows.Overlapped{}
	e = windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, overlapped)
	if e != nil {
		f.Close()
		return nil, fmt.Errorf("папка занята другим запуском: %w", e)
	}
	return func() { windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, overlapped); f.Close() }, nil
}
