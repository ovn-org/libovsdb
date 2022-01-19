package util

import (
	"fmt"
	"strings"
)

func CombineErrors(errors []error, msg string) error {
	if len(errors) == 0 {
		return nil
	} else if len(errors) == 1 {
		return errors[0]
	}

	var combined []string
	for _, e := range errors {
		combined = append(combined, e.Error())
	}
	return fmt.Errorf("%s: %s", msg, strings.Join(combined, ". "))
}
