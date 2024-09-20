package utils

import (
	"fmt"
)

const printLimit = 100

func StringData(p []byte) string {
	if len(p) > printLimit {
		p = p[:printLimit]
	}
	return fmt.Sprintf("[%x]", p)
}
