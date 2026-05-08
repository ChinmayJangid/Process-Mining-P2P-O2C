"""
Main Application Entry Point — Process Mining Backend
Run: uvicorn app:app --reload --port 8000
All outputs, audits, and errors will be displayed in this terminal.
"""

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime

# Import the modular routers
from o2c4 import router as o2c_router
from p2p4 import router as p2p_router
from p2ptransformer import transformer_router   # ← P2P
from o2ctransformer import o2c_transformer_router   # ← O2C
from p2i4 import router as p2i_router
from p2itransformer import p2i_transformer_router # ← P2I

app = FastAPI(title="Process Mining Dynamic API (O2C & P2P)")

# Global CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

print("\n" + "="*50)
print("[INFO] Unified Process Mining Backend Initialized")
print("[INFO] Modules Loaded: O2C, P2P, P2I, P2P-Transformer, O2C-Transformer, P2I-Transformer")
print("[INFO] All outputs and logs will stream directly to this terminal.")
print("="*50 + "\n")

# ─── Shared Authentication ───────────────────────────────────────────────────
class LoginData(BaseModel):
    username: str
    password: str

ALLOWED_USERS = {"chinmay": "securepass123", "admin": "admin123", "ajalabs": "data2026"}

@app.post("/login")
def login(data: LoginData):
    if data.username in ALLOWED_USERS and ALLOWED_USERS[data.username] == data.password:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[AUDIT] {timestamp} | {data.username} | LOGIN | User logged in.")
        return {"status": "ok", "username": data.username, "token": "mock-jwt-token"}
    print(f"[WARNING] Failed login attempt for username: {data.username}")
    raise HTTPException(status_code=401, detail="Invalid username or password.")

# ─── Mount Routers ───────────────────────────────────────────────────────────
app.include_router(o2c_router)
app.include_router(p2p_router)
app.include_router(transformer_router)      # ← P2P
app.include_router(o2c_transformer_router)   # ← O2C
app.include_router(p2i_router)               # ← P2I
app.include_router(p2i_transformer_router)   # ← P2I-Transformer

@app.get("/")
def root():
    return {
        "status": "Unified API running",
        "modules_available": ["/o2c", "/p2p", "/p2i", "/p2p/transform", "/o2c/transform", "/p2i/transform"]
    }

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)