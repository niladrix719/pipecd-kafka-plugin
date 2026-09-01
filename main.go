package main

import (
	"log"

	sdk "github.com/pipe-cd/piped-plugin-sdk-go"

	"github.com/niladrix719/pipecd-kafka-plugin/deployment"
)

// version is the plugin version reported to piped.
const version = "1.0.12"

func main() {
	// One instance serves both roles: the live-state view builds the same plan
	// the deployment stages do, against the same cluster.
	kafka := &deployment.Plugin{}

	plugin, err := sdk.NewPlugin(version,
		sdk.WithDeploymentPlugin(kafka),
		sdk.WithLivestatePlugin(kafka),
	)
	if err != nil {
		log.Fatalln(err)
	}
	if err := plugin.Run(); err != nil {
		log.Fatalln(err)
	}
}
