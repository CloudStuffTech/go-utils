package jwt

import (
	"fmt"

	jwtLib "github.com/golang-jwt/jwt"
)

func ParseJWT(jwtToken, secretKey string) (jwtLib.MapClaims, error) {
	token, err := jwtLib.Parse(jwtToken, func(token *jwtLib.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwtLib.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secretKey), nil
	})
	if claims, ok := token.Claims.(jwtLib.MapClaims); ok && token.Valid {
		return claims, nil
	}
	return nil, err
}
