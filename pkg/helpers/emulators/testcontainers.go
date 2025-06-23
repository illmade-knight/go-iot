package emulators

type ImageContainer struct {
	EmulatorImage    string
	EmulatorPort     string
	EmulatorGRPCPort string
}

type GCImageContainer struct {
	ImageContainer
	ProjectID       string
	SetEnvVariables bool
}
