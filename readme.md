# Summary 
This app
1. Creates few Producers
2. Creates few consumers along with consumer groups
3. Producer produces message after passing the given interval periodically
4. Consumer consumes those messages and logs the value

# Guide to run the App

1. Install and Run Kafka on default address i.e. localhost:9092
2. Build Project with `go build .`
3. Run the Project by calling the new binary
4. Quit the App with `Ctrl+C`