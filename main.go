package main

import (
	"log"

	sdk "github.com/pipe-cd/piped-plugin-sdk-go"

	"github.com/niladrix719/pipecd-kafka-plugin/deployment"
)

// version is the plugin version reported to piped.
const version = "0.1.0"

func main() {
	plugin, err := sdk.NewPlugin(version, sdk.WithDeploymentPlugin(&deployment.Plugin{}))
	if err != nil {
		log.Fatalln(err)
	}
	if err := plugin.Run(); err != nil {
		log.Fatalln(err)
	}
}
