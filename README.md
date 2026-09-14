## PhotonCore

The JVM application underlying the desktop PhotonFlight app.

---

### Responsibilities

- Holding the backing data for the app
- Saving backing data to our persistence layer (likely PostgreSQL with PostGIS)
- Providing the plugin API
- Setting Guice and providing dependencies
- Quickly providing backing data to the UI layer
- Receiving simple instructions from the UI layer