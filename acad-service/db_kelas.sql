CREATE TABLE mahasiswa (
    nim VARCHAR(10) PRIMARY KEY,
    nama VARCHAR(100) NOT NULL,
    jurusan VARCHAR(50),
    angkatan INT
);

CREATE TABLE mata_kuliah (
    kode_mk VARCHAR(10) PRIMARY KEY,
    nama_mk VARCHAR(100) NOT NULL,
    sks INT NOT NULL
);

CREATE TABLE krs (
    id_krs bigserial PRIMARY KEY,
    nim VARCHAR(10),
    kode_mk VARCHAR(10),
    nilai CHAR(2),
    semester INT,
    FOREIGN KEY (nim) REFERENCES mahasiswa(nim),
    FOREIGN KEY (kode_mk) REFERENCES mata_kuliah(kode_mk)
);

CREATE TABLE bobot_nilai (
    nilai CHAR(2) PRIMARY KEY,
    bobot FLOAT
);


INSERT INTO mahasiswa (nim, nama, jurusan, angkatan) VALUES
('22001', 'Ahmad Fauzan', 'Sains Data', 2022),
('22002', 'Nisa Rahma', 'Sains Data', 2022);

INSERT INTO mata_kuliah (kode_mk, nama_mk, sks) VALUES
('IF101', 'Pemrograman Dasar', 3),
('IF102', 'Basis Data', 3),
('IF103', 'Aljabar Matriks', 3);

INSERT INTO krs (nim, kode_mk, nilai, semester) VALUES
('22001', 'IF101', 'A', 1),
('22001', 'IF102', 'B+', 1),
('22001', 'IF103', 'B', 1),

('22002', 'IF101', 'A', 1),
('22002', 'IF102', 'A', 1),
('22002', 'IF103', 'B+', 1);

INSERT INTO bobot_nilai VALUES
('A', 4.0),
('A-', 3.75),
('B+', 3.5),
('B', 3.0),
('B-', 2.75),
('C+', 2.5),
('C',2.0),
('D', 1.0),
('E', 0.0);
