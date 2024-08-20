package sql

import (
	"fmt"
	"strings"

	"github.com/CloudStuffTech/go-utils/security"
)

func WrapIntArrValue(intArr []int) string {
	var arr []string
	for _, v := range intArr {
		arr = append(arr, fmt.Sprintf("%d", v))
	}
	val := strings.Join(arr, ",")
	return fmt.Sprintf("(%s)", val)
}

func WrapStrArrValue(strArr []string) string {
	var arr []string
	for _, v := range strArr {
		arr = append(arr, WrapStrValue(v))
	}
	val := strings.Join(arr, ",")
	return fmt.Sprintf("(%s)", val)
}

func WrapStrValue(str string) string {
	return wrapValue(security.AddSlashes(str))
}

func wrapValue(str string) string {
	return fmt.Sprintf("'%s'", str)
}
