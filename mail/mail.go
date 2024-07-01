package mail

import (
	"context"
	"time"

	"github.com/mailgun/mailgun-go/v4"
	mailjet "github.com/mailjet/mailjet-apiv3-go"
)

type Config struct {
	Key, Domain string
}

type MailjetConfig struct {
	PubKey, PrivateKey string
}

type Params struct {
	Sender, Subject string
	Body, Recipient string
	ReplyTo         string
	CC, BCC         []string // CC emails
	Timeout         int      // timeout in seconds
}

type Attachment struct {
	Filename string
	Content  string
}

type MailjetParams struct {
	SenderEmail, SenderName, ReplyToEmail string
	RecipientEmail                        []string
	Subject                               string
	CC, BCC                               []string
	TextPart, HtmlPart                    string
	Attachments                           []Attachment
}

// SendViaMailgun will try to send the mail using mailgun
func SendViaMailgun(conf *Config, params *Params) (string, string, error) {
	mg := mailgun.NewMailgun(conf.Domain, conf.Key)
	message := mg.NewMessage(params.Sender, params.Subject, params.Body, params.Recipient)
	message.SetHtml(params.Body)
	if len(params.ReplyTo) > 0 {
		message.SetReplyTo(params.ReplyTo)
	}

	for _, emailID := range params.CC {
		message.AddCC(emailID)
	}
	for _, emailID := range params.BCC {
		message.AddBCC(emailID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	resp, id, err := mg.Send(ctx, message)
	return resp, id, err
}

// SendViaMailjet will try to send the mail using mailjet
func SendViaMailjet(conf *MailjetConfig, params *MailjetParams) (*mailjet.ResultsV31, error) {
	mailjetClient := mailjet.NewMailjetClient(conf.PubKey, conf.PrivateKey)
	var toMailjetRecepient, ccMailjetRecepient, bccMailjetRecepient mailjet.RecipientsV31

	for _, emailID := range params.RecipientEmail {
		toMailjetRecepient = append(toMailjetRecepient, mailjet.RecipientV31{
			Email: emailID,
		})
	}

	for _, emailID := range params.CC {
		ccMailjetRecepient = append(ccMailjetRecepient, mailjet.RecipientV31{
			Email: emailID,
		})
	}

	for _, emailID := range params.BCC {
		bccMailjetRecepient = append(bccMailjetRecepient, mailjet.RecipientV31{
			Email: emailID,
		})
	}

	htmlContent := params.HtmlPart
	msg := mailjet.InfoMessagesV31{
		From: &mailjet.RecipientV31{
			Email: params.SenderEmail,
			Name:  params.SenderName,
		},
		ReplyTo: &mailjet.RecipientV31{
			Email: params.ReplyToEmail,
		},
		To:       &toMailjetRecepient,
		Cc:       &ccMailjetRecepient,
		Bcc:      &bccMailjetRecepient,
		Subject:  params.Subject,
		TextPart: params.TextPart,
		HTMLPart: htmlContent,
	}

	if len(params.Attachments) > 0 {
		var result mailjet.AttachmentsV31
		for _, atch := range params.Attachments {
			at := mailjet.AttachmentV31{
				Filename:      atch.Filename,
				Base64Content: atch.Content,
			}
			result = append(result, at)
		}
		msg.Attachments = &result
	}

	messagesInfo := []mailjet.InfoMessagesV31{msg}
	messages := mailjet.MessagesV31{Info: messagesInfo}
	res, err := mailjetClient.SendMailV31(&messages)
	return res, err
}
