from fastapi import FastAPI

# Import the FastAPI applications
from api1 import app as api1_app
from ap2 import app as api2_app
from api3 import app as api3_app
from api4 import app as api4_app
from api5 import app as api5_app
from api6 import app as api6_app
from fastapi.middleware.cors import CORSMiddleware

# Main FastAPI application
app = FastAPI(
    title="Telecom Network Analytics API",
    version="1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# Include routes from API 1
# ============================================================

for route in api1_app.routes:
    app.router.routes.append(route)


# ============================================================
# Include routes from API 2
# ============================================================

for route in api2_app.routes:
    app.router.routes.append(route)


# ============================================================
# Include routes from API 3
# ============================================================

for route in api3_app.routes:
    app.router.routes.append(route)


# ============================================================
# Include routes from API 4
# ============================================================

for route in api4_app.routes:
    app.router.routes.append(route)


# ============================================================
# Include routes from API 5
# ============================================================

for route in api5_app.routes:
    app.router.routes.append(route)

# API6: grid location, anomaly evidence and pipeline status
for route in api6_app.routes:
    app.router.routes.append(route)


# ============================================================
# Health check
# ============================================================

@app.get("/")
def root():
    return {
        "message": "Telecom Network Analytics API is running",
        "status": "healthy"
    }
