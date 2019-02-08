# EventHorizon with PostgreSQL

## Features

- EventStore

```golang
db := pg.Connect(&pg.Options{
  Addr:     os.Getenv("POSTGRES_ADDR"),
  Database: os.Getenv("POSTGRES_DB"),
  User:     os.Getenv("POSTGRES_USER"),
  Password: os.Getenv("POSTGRES_PASSWORD"),
})
defer db.Close()

store, err := ehpg.NewEventStore(db)
```