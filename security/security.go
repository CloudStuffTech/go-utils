package security

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	cryptoRand "crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"time"

	"crypto/md5"
	"hash/fnv"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

var (
	secretKey = []byte("vnnaEPK8CJbXGuSk2qa9Zh2VetP")
)

func RandomInt(min, max int) int {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	return r1.Intn(max-min) + min
}

func Sha256(message []byte) string {
	mac := hmac.New(sha256.New, secretKey)
	mac.Write(message)
	expectedMAC := mac.Sum(nil)
	return hex.EncodeToString(expectedMAC)
}

func Sha256Hmac(message, key []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(message)
	expectedMAC := mac.Sum(nil)
	return expectedMAC
}

func MakeTrackSecret(message string) string {
	msg := []byte(message)
	hash := Sha256(msg)
	return hash
}

func Base64Decode(str string) string {
	sDec, _ := base64.StdEncoding.DecodeString(str)
	return string(sDec)
}

func Base64Encode(data string) string {
	enc := base64.StdEncoding.EncodeToString([]byte(data))
	return enc
}

func Base64EncodeRaw(data []byte) string {
	enc := base64.RawURLEncoding.EncodeToString(data)
	return enc
}

func MD5(data []byte) string {
	return fmt.Sprintf("%x", md5.Sum(data))
}

func RandomString(n int) string {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

// EncryptText method will encrypt the text using the key provided with
// AES algo. Ref: https://tutorialedge.net/golang/go-encrypt-decrypt-aes-tutorial/
func EncryptText(text, key []byte) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	_, err = io.ReadFull(cryptoRand.Reader, nonce)
	if err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, text, nil), nil
}

// HashStr method will hash the string to int32 value
func HashStr(value string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(value))
	return h.Sum32()
}

// DecryptText method will decrypt the text with help of a key
// Ref: https://tutorialedge.net/golang/go-encrypt-decrypt-aes-tutorial/
func DecryptText(ciphertext, key []byte) ([]byte, error) {
	c, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(c)
	if err != nil {
		return nil, err
	}
	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, errors.New("Invalid ciphertext")
	}
	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)

	return plaintext, err
}

func Sha512Hash(value string) string {
	h := sha512.New()
	h.Write([]byte(value))
	return hex.EncodeToString(h.Sum(nil))
}
