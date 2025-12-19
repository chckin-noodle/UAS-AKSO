from fastapi import FastAPI, HTTPException, Query, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from typing import Optional, List
import psycopg2
from psycopg2.extras import RealDictCursor
import os
from datetime import datetime
from contextlib import contextmanager
import httpx

app = FastAPI(title="Academic Service", version="1.0.0")

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST', 'localhost'),
    'port': os.getenv('DB_PORT', '5432'),
    'database': os.getenv('DB_NAME', 'academic'),
    'user': os.getenv('DB_USER', 'acaduser'),
    'password': os.getenv('DB_PASSWORD', 'acadpass')
}

AUTH_SERVICE_URL = os.getenv('AUTH_SERVICE_URL', 'http://auth-service:3001')

# Models
class Mahasiswa(BaseModel):
    nim: str
    nama: str
    jurusan: str
    angkatan: int = Field(ge=2000, le=2100)

class MataKuliah(BaseModel):
    kode_mk: str
    nama_mk: str
    sks: int = Field(ge=1, le=6)

class KRS(BaseModel):
    nim: str
    kode_mk: str
    nilai: Optional[str] = None
    semester: int = Field(ge=1, le=14)

# Database connection pool
@contextmanager
def get_db_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

# Auth verification
async def verify_admin(authorization: Optional[str] = Header(None)):
    """Verify bahwa user adalah admin"""
    if not authorization:
        raise HTTPException(status_code=401, detail="No authorization header")
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{AUTH_SERVICE_URL}/api/auth/verify",
                headers={"Authorization": authorization},
                timeout=5.0
            )
            
            if response.status_code != 200:
                raise HTTPException(status_code=401, detail="Invalid token")
            
            data = response.json()
            user = data.get('user')
            
            if not user or user.get('role') != 'admin':
                raise HTTPException(status_code=403, detail="Access denied. Admin only.")
            
            return user
    except httpx.RequestError:
        raise HTTPException(status_code=503, detail="Auth service unavailable")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=401, detail=str(e))

@app.on_event("startup")
async def startup_event():
    try:
        with get_db_connection() as conn:
            print("Acad Service: Connected to PostgreSQL")
    except Exception as e:
        print(f"Acad Service: PostgreSQL connection error: {e}")

# Serve static files
app.mount("/static", StaticFiles(directory="/app/static"), name="static")

@app.get("/")
async def root():
    return FileResponse('/app/static/index.html')

# Health check
@app.get("/health")
async def health_check():
    return {
        "status": "Acad Service is running",
        "timestamp": datetime.now().isoformat()
    }

# ==================== MAHASISWA ENDPOINTS ====================

@app.get("/api/acad/mahasiswa")
async def get_mahasiswas():
    """Mendapatkan semua data mahasiswa"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM mahasiswa ORDER BY nim"
            cursor.execute(query)
            rows = cursor.fetchall()

            return [{"nim": row[0], "nama": row[1], "jurusan": row[2], "angkatan": row[3]} for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/acad/mahasiswa", status_code=201)
async def create_mahasiswa(
    mahasiswa: Mahasiswa,
    admin: dict = Depends(verify_admin)
):
    """Menambahkan mahasiswa baru (Admin only)"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Cek apakah NIM sudah ada
            cursor.execute("SELECT nim FROM mahasiswa WHERE nim = %s", (mahasiswa.nim,))
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="NIM sudah terdaftar")
            
            query = """
                INSERT INTO mahasiswa (nim, nama, jurusan, angkatan)
                VALUES (%s, %s, %s, %s)
                RETURNING nim, nama, jurusan, angkatan
            """
            cursor.execute(query, (
                mahasiswa.nim,
                mahasiswa.nama,
                mahasiswa.jurusan,
                mahasiswa.angkatan
            ))
            
            row = cursor.fetchone()
            
            return {
                "message": "Mahasiswa berhasil ditambahkan",
                "data": {
                    "nim": row[0],
                    "nama": row[1],
                    "jurusan": row[2],
                    "angkatan": row[3]
                },
                "created_by": admin.get('username')
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== MATA KULIAH ENDPOINTS ====================

@app.get("/api/acad/mata-kuliah")
async def get_mata_kuliah():
    """Mendapatkan semua data mata kuliah"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM mata_kuliah ORDER BY kode_mk"
            cursor.execute(query)
            rows = cursor.fetchall()

            return [{
                "kode_mk": row[0],
                "nama_mk": row[1],
                "sks": row[2]
            } for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/acad/mata-kuliah", status_code=201)
async def create_mata_kuliah(
    mata_kuliah: MataKuliah,
    admin: dict = Depends(verify_admin)
):
    """Menambahkan mata kuliah baru (Admin only)"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Cek apakah kode MK sudah ada
            cursor.execute("SELECT kode_mk FROM mata_kuliah WHERE kode_mk = %s", (mata_kuliah.kode_mk,))
            if cursor.fetchone():
                raise HTTPException(status_code=400, detail="Kode mata kuliah sudah ada")
            
            query = """
                INSERT INTO mata_kuliah (kode_mk, nama_mk, sks)
                VALUES (%s, %s, %s)
                RETURNING kode_mk, nama_mk, sks
            """
            cursor.execute(query, (
                mata_kuliah.kode_mk,
                mata_kuliah.nama_mk,
                mata_kuliah.sks
            ))
            
            row = cursor.fetchone()
            
            return {
                "message": "Mata kuliah berhasil ditambahkan",
                "data": {
                    "kode_mk": row[0],
                    "nama_mk": row[1],
                    "sks": row[2]
                },
                "created_by": admin.get('username')
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== NILAI (KRS) ENDPOINTS ====================

@app.get("/api/acad/nilai")
async def get_nilai(nim: Optional[str] = None, semester: Optional[int] = None):
    """Mendapatkan data nilai mahasiswa dengan filter opsional"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            query = """
                SELECT k.id_krs, k.nim, m.nama, mk.kode_mk, mk.nama_mk, 
                       mk.sks, k.nilai, k.semester, bn.bobot
                FROM krs k
                JOIN mahasiswa m ON k.nim = m.nim
                JOIN mata_kuliah mk ON k.kode_mk = mk.kode_mk
                LEFT JOIN bobot_nilai bn ON k.nilai = bn.nilai
                WHERE 1=1
            """
            params = []
            
            if nim:
                query += " AND k.nim = %s"
                params.append(nim)
            
            if semester:
                query += " AND k.semester = %s"
                params.append(semester)
            
            query += " ORDER BY k.nim, k.semester, mk.kode_mk"
            
            cursor.execute(query, params)
            rows = cursor.fetchall()

            return [{
                "id_krs": row[0],
                "nim": row[1],
                "nama_mahasiswa": row[2],
                "kode_mk": row[3],
                "nama_mk": row[4],
                "sks": row[5],
                "nilai": row[6],
                "semester": row[7],
                "bobot": float(row[8]) if row[8] else 0.0
            } for row in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/acad/nilai", status_code=201)
async def create_nilai(
    krs: KRS,
    admin: dict = Depends(verify_admin)
):
    """Menambahkan nilai/KRS baru (Admin only)"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Cek apakah mahasiswa ada
            cursor.execute("SELECT nim FROM mahasiswa WHERE nim = %s", (krs.nim,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Mahasiswa tidak ditemukan")
            
            # Cek apakah mata kuliah ada
            cursor.execute("SELECT kode_mk FROM mata_kuliah WHERE kode_mk = %s", (krs.kode_mk,))
            if not cursor.fetchone():
                raise HTTPException(status_code=404, detail="Mata kuliah tidak ditemukan")
            
            # Cek apakah sudah pernah mengambil MK di semester yang sama
            cursor.execute(
                "SELECT id_krs FROM krs WHERE nim = %s AND kode_mk = %s AND semester = %s",
                (krs.nim, krs.kode_mk, krs.semester)
            )
            if cursor.fetchone():
                raise HTTPException(
                    status_code=400, 
                    detail="Mahasiswa sudah mengambil mata kuliah ini di semester yang sama"
                )
            
            query = """
                INSERT INTO krs (nim, kode_mk, nilai, semester)
                VALUES (%s, %s, %s, %s)
                RETURNING id_krs, nim, kode_mk, nilai, semester
            """
            cursor.execute(query, (
                krs.nim,
                krs.kode_mk,
                krs.nilai,
                krs.semester
            ))
            
            row = cursor.fetchone()
            
            return {
                "message": "Nilai/KRS berhasil ditambahkan",
                "data": {
                    "id_krs": row[0],
                    "nim": row[1],
                    "kode_mk": row[2],
                    "nilai": row[3],
                    "semester": row[4]
                },
                "created_by": admin.get('username')
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==================== IPS & IPK ENDPOINTS ====================

@app.get("/api/acad/ips/{nim}")
async def hitung_ips(nim: str, semester: int = Query(..., ge=1, description="Semester yang akan dihitung IPS-nya")):
    """Menghitung IPS (Indeks Prestasi Semester) mahasiswa"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Cek apakah mahasiswa ada
            cursor.execute("SELECT * FROM mahasiswa WHERE nim = %s", (nim,))
            mahasiswa = cursor.fetchone()
            
            if not mahasiswa:
                raise HTTPException(status_code=404, detail="Mahasiswa tidak ditemukan")
            
            # Ambil data nilai untuk semester tertentu
            query = """
                SELECT k.kode_mk, mk.nama_mk, mk.sks, k.nilai, bn.bobot
                FROM krs k
                JOIN mata_kuliah mk ON k.kode_mk = mk.kode_mk
                LEFT JOIN bobot_nilai bn ON k.nilai = bn.nilai
                WHERE k.nim = %s AND k.semester = %s
            """
            
            cursor.execute(query, (nim, semester))
            rows = cursor.fetchall()
            
            if not rows:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Tidak ada data nilai untuk mahasiswa {nim} di semester {semester}"
                )
            
            # Hitung IPS
            total_mutu = 0.0
            total_sks = 0
            mata_kuliah_list = []
            
            for row in rows:
                kode_mk = row[0]
                nama_mk = row[1]
                sks = row[2]
                nilai = row[3]
                bobot = float(row[4]) if row[4] else 0.0
                
                mutu = bobot * sks
                total_mutu += mutu
                total_sks += sks
                
                mata_kuliah_list.append({
                    "kode_mk": kode_mk,
                    "nama_mk": nama_mk,
                    "sks": sks,
                    "nilai": nilai,
                    "bobot": bobot,
                    "mutu": mutu
                })
            
            ips = total_mutu / total_sks if total_sks > 0 else 0.0
            
            return {
                "nim": nim,
                "nama": mahasiswa[1],
                "semester": semester,
                "mata_kuliah": mata_kuliah_list,
                "total_sks": total_sks,
                "total_mutu": round(total_mutu, 2),
                "ips": round(ips, 2)
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/acad/ipk/{nim}")
async def hitung_ipk(nim: str):
    """Menghitung IPK (Indeks Prestasi Kumulatif) mahasiswa"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Cek apakah mahasiswa ada
            cursor.execute("SELECT * FROM mahasiswa WHERE nim = %s", (nim,))
            mahasiswa = cursor.fetchone()
            
            if not mahasiswa:
                raise HTTPException(status_code=404, detail="Mahasiswa tidak ditemukan")
            
            # Ambil semua data nilai mahasiswa
            query = """
                SELECT k.semester, k.kode_mk, mk.nama_mk, mk.sks, k.nilai, bn.bobot
                FROM krs k
                JOIN mata_kuliah mk ON k.kode_mk = mk.kode_mk
                LEFT JOIN bobot_nilai bn ON k.nilai = bn.nilai
                WHERE k.nim = %s
                ORDER BY k.semester
            """
            
            cursor.execute(query, (nim,))
            rows = cursor.fetchall()
            
            if not rows:
                raise HTTPException(
                    status_code=404, 
                    detail=f"Tidak ada data nilai untuk mahasiswa {nim}"
                )
            
            # Hitung IPK dan IPS per semester
            total_mutu_kumulatif = 0.0
            total_sks_kumulatif = 0
            per_semester = {}
            
            for row in rows:
                semester = row[0]
                kode_mk = row[1]
                nama_mk = row[2]
                sks = row[3]
                nilai = row[4]
                bobot = float(row[5]) if row[5] else 0.0
                
                mutu = bobot * sks
                total_mutu_kumulatif += mutu
                total_sks_kumulatif += sks
                
                if semester not in per_semester:
                    per_semester[semester] = {
                        "semester": semester,
                        "total_sks": 0,
                        "total_mutu": 0.0,
                        "ips": 0.0
                    }
                
                per_semester[semester]["total_sks"] += sks
                per_semester[semester]["total_mutu"] += mutu
            
            # Hitung IPS per semester
            for semester in per_semester:
                total_sks = per_semester[semester]["total_sks"]
                total_mutu = per_semester[semester]["total_mutu"]
                per_semester[semester]["ips"] = round(total_mutu / total_sks if total_sks > 0 else 0.0, 2)
                per_semester[semester]["total_mutu"] = round(total_mutu, 2)
            
            ipk = total_mutu_kumulatif / total_sks_kumulatif if total_sks_kumulatif > 0 else 0.0
            
            return {
                "nim": nim,
                "nama": mahasiswa[1],
                "jurusan": mahasiswa[2],
                "angkatan": mahasiswa[3],
                "total_sks_kumulatif": total_sks_kumulatif,
                "total_mutu_kumulatif": round(total_mutu_kumulatif, 2),
                "ipk": round(ipk, 2),
                "detail_per_semester": sorted(per_semester.values(), key=lambda x: x["semester"])
            }
            
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
