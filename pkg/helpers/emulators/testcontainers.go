package emulators

type ImageContainer struct {
	EmulatorImage    string
	EmulatorHTTPPort string
	EmulatorGRPCPort string
}

type GCImageContainer struct {
	ImageContainer
	ProjectID       string
	SetEnvVariables bool
}
