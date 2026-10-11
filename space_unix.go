//go:build !windows

package main

import "golang.org/x/sys/unix"

func volumeSpace(root string) (uint64, uint64, error) {
	var st unix.Statfs_t
	e := unix.Statfs(root, &st)
	return uint64(st.Blocks) * uint64(st.Bsize), uint64(st.Bfree) * uint64(st.Bsize), e
}
