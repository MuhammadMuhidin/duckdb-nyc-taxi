🚖 NYC Yellow Taxi Data Transformer

Script Python untuk mengambil, memfilter, dan men-transformasi data perjalanan Yellow Taxi NYC secara bulanan dari sumber cloud resmi, lalu menyimpannya dalam format Parquet, dipartisi berdasarkan bulan, menggunakan DuckDB.


---

📦 Dependencies

Install terlebih dahulu dependensi berikut:

pip install duckdb==1.3.2 requests==2.32.4


---

🚀 Cara Pakai

1. Clone repository ini:



git clone https://github.com/muhammadmuhidin/duckdb-nyc-taxi.git
cd duckdb-nyc-taxi

2. Jalankan script:



python main.py

Secara default, data tahun 2025 akan diproses dan disimpan ke folder lokal /content/monthly_data.


---

⚙️ Konfigurasi

Edit baris berikut di main.py untuk menyesuaikan kebutuhanmu:

transformer = TripDataTransformer(
    year=2025,
    output_path="/content/monthly_data"
)

Parameter yang tersedia:

year: Tahun data yang ingin diproses (int)

output_path: Lokasi penyimpanan output (string)

base_url: (opsional) Sumber data jika bukan default NYC Taxi (string)



---

📁 Output

Data akan disimpan dalam format Parquet, otomatis dipartisi berdasarkan bulan seperti:

/content/monthly_data/month=2025-01/data-0.parquet
/content/monthly_data/month=2025-02/data-0.parquet
...


---

✅ Fitur

✅ Cek otomatis apakah file tersedia sebelum diproses (skip 404)

✅ Output efisien format parquet partisi bulanan

✅ Clean code berbasis OOP (class TripDataTransformer)

✅ Mudah diperluas untuk dataset lain atau tahun berbeda
