# go-iot
a golang iot library derived from our ml agent experiments

pkg has reusable elements that can be combined to form iot microservice pipelines

to see a pipeline example see the [gardenmonitor](https://github.com/illmade-knight/go-iot-dataflows/tree/main/gardenmonitor)

### Agent driven

This is still driven by the ideas in our Agent based coding experiments and I'm
trying to keep to nearly all the code being created by Gemini.

At the moment this requires significant oversight and re-prompting.

There are particular issues with refactoring when the agent sometimes throws code away...

### Testing

The tests are also agent created - I notice a significant decrease in code quality and accuracy 
when asking for tests and it needs careful prompting to use better practice.

### Why Agents

at the moment it can be a bit painful 

Sometimes it allows very rapid coding - sometimes I get bogged down in negative interactions with the clients

But basically I'm sticking with it because I believe this is the way most code will be written.

I've gone through more detailed steps in the [TDD](https://github.com/illmade-knight/ai-tdd) and [MVP](https://github.com/illmade-knight/ai-power-mvp)

so I haven't commented much on the process here - and the process changes so rapidly I'm not sure how much benefit there is
in commenting on it unless I plan to constantly update 
(welcome to the rapid changes in the ML/AI world where 6 months ago is ancient history)
