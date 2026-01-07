from fastapi import FastAPI, Request, UploadFile, File, Depends, HTTPException, status
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.staticfiles import StaticFiles
from typing import List
import pandas as pd
import io
import traceback
import re
import os
import secrets
from datetime import datetime

app = FastAPI()
app.mount("/static", StaticFiles(directory="app/static"), name="static")
security = HTTPBasic()
templates = Jinja2Templates(directory="app/templates")

# --- НАСТРОЙКИ БЕЗОПАСНОСТИ ---
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "secret")

def get_current_username(credentials: HTTPBasicCredentials = Depends(security)):
    is_user_ok = secrets.compare_digest(credentials.username, ADMIN_USER)
    is_pass_ok = secrets.compare_digest(credentials.password, ADMIN_PASS)
    
    if not (is_user_ok and is_pass_ok):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username

# --- ФИЛЬТР (СТАНДАРТНЫЙ, 0,00) ---
def format_rub(value):
    if value is None or value == "": return "0,00"
    try: val = float(value)
    except: return "0,00"
    
    s = "{:,.2f}".format(val)
    # Заменяем запятую на пробел, точку на запятую
    return s.replace(",", "SPACE").replace(".", ",").replace("SPACE", " ")

# Регистрируем фильтр
templates.env.filters["rub"] = format_rub
# На всякий случай регистрируем rub_dash как rub, чтобы не ломался старый шаблон, если он остался
templates.env.filters["rub_dash"] = format_rub

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---
def clean_money(val):
    if pd.isna(val) or val == "": return 0.0
    if isinstance(val, (int, float)): return float(val)
    s = str(val).replace(" ", "").replace("\xa0", "").replace(",", ".")
    try: return float(s)
    except ValueError: return 0.0

def time_to_hours(val):
    if pd.isna(val) or val == "": return 0.0
    s = str(val).strip()
    if ":" in s:
        try:
            parts = s.split(":")
            return int(parts[0]) + int(parts[1]) / 60.0
        except: return 0.0
    try: return float(s)
    except: return 0.0

def get_rus_day(date_str, year_str):
    try:
        full_date = f"{date_str}.{year_str}"
        dt = datetime.strptime(full_date, "%d.%m.%Y")
        days = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
        return " " + days[dt.weekday()]
    except: return ""

def parse_single_file(file_bytes, filename):
    print(f"--- ОБРАБОТКА: {filename} ---")
    if filename.endswith(".csv"):
        try: df = pd.read_csv(io.BytesIO(file_bytes), header=None, dtype=str)
        except: df = pd.read_csv(io.BytesIO(file_bytes), header=None, dtype=str, encoding='cp1251', sep=';')
    else:
        df = pd.read_excel(io.BytesIO(file_bytes), header=None, dtype=str)

    header_row_idx = 0
    for idx, row in df.iterrows():
        row_str = " ".join(row.fillna("").astype(str).values).lower()
        if "явки" in row_str or "засч" in row_str:
            header_row_idx = idx
            break
    
    raw_name = filename.rsplit('.', 1)[0]
    try:
        val_a1 = str(df.iloc[0, 0])
        if " : " in val_a1:
            candidate = val_a1.split(" : ")[0].strip()
            if len(candidate) > 2: raw_name = candidate
        else:
            if "_" in filename: raw_name = filename.split("_")[0]
    except: pass

    raw_period = "Период не определен"
    period_year = str(datetime.now().year)
    meta_df = df.iloc[:header_row_idx+1]
    for idx, row in meta_df.iterrows():
        txt = " ".join(row.fillna("").astype(str).values)
        period_match = re.search(r'с\s+(\d{2}\.\d{2}\.\d{4})\s+по\s+(\d{2}\.\d{2}\.\d{4})', txt)
        if period_match:
            raw_period = f"{period_match.group(1)} - {period_match.group(2)}"
            period_year = period_match.group(1).split('.')[-1]
            break

    data_df = df.iloc[header_row_idx+1:].copy()
    COL_DATE, COL_TIME, COL_HOURS = 0, 1, 3
    COL_BONUS, COL_DEDUCT, COL_ADVANCE, COL_TOTAL = 4, 5, 6, 7

    employees = []
    current_emp = {
        "name": raw_name, "period": raw_period, "start_balance": 0.0,
        "shifts": [], "advances": [], "deductions": [], "bonuses": [],
        "total_accrued": 0.0, "total_payout": 0.0, "calc_payout": 0.0,
        "total_hours": 0.0, "total_shift_pay": 0.0 # <--- НОВЫЕ ПОЛЯ
    }
    clean_rates = []

    for idx, row in data_df.iterrows():
        def get_val(c_idx): return row[c_idx] if c_idx in row else ""
        c_date = str(get_val(COL_DATE)) if not pd.isna(get_val(COL_DATE)) else ""
        c_hours = time_to_hours(get_val(COL_HOURS))
        c_bonus = clean_money(get_val(COL_BONUS))
        c_deduct = clean_money(get_val(COL_DEDUCT))
        c_advance = clean_money(get_val(COL_ADVANCE))
        c_total = clean_money(get_val(COL_TOTAL))

        if "НА НАЧАЛО" in c_date.upper():
            current_emp["start_balance"] = c_total
            continue
        if "НА КОНЕЦ" in c_date.upper():
            current_emp["total_payout"] = c_total
            continue
        if "ИТОГО" in c_date.upper() and len(c_date) < 15: continue

        if any(char.isdigit() for char in c_date):
            clean_date_str = c_date.split(",")[0].strip()
            is_debt_repayment = False
            if current_emp["start_balance"] > 0 and c_advance < 0:
                if abs(abs(c_advance) - current_emp["start_balance"]) < 2.0: is_debt_repayment = True
            
            if c_bonus != 0: current_emp["bonuses"].append({"date": clean_date_str, "amount": c_bonus})
            
            if c_hours > 0:
                is_source_for_rate = (c_deduct == 0) and (c_advance == 0)
                earnings = 0.0
                if is_source_for_rate:
                    pure_hours_pay = c_total - c_bonus
                    if pure_hours_pay > 0:
                        rate = pure_hours_pay / c_hours
                        clean_rates.append(rate)
                    earnings = pure_hours_pay
                
                weekday_suffix = get_rus_day(clean_date_str, period_year)
                current_emp["shifts"].append({
                    "date": clean_date_str + weekday_suffix, "time": str(get_val(COL_TIME)) if not pd.isna(get_val(COL_TIME)) else "",
                    "hours": c_hours, "row_total_source": c_total, "deduct": c_deduct, "advance": c_advance, "bonus_in_row": c_bonus,
                    "is_source_for_rate": is_source_for_rate, "earnings": earnings
                })
            else:
                if c_deduct != 0: current_emp["deductions"].append({"date": clean_date_str, "amount": c_deduct})
                if c_advance != 0 and not is_debt_repayment: current_emp["advances"].append({"date": clean_date_str, "amount": c_advance})

    avg_rate = 0.0
    if clean_rates:
        clean_rates.sort()
        avg_rate = clean_rates[len(clean_rates)//2]
    
    hours_earnings_sum = 0.0
    total_hours_sum = 0.0 # <--- СЧЕТЧИК ЧАСОВ

    for shift in current_emp["shifts"]:
        if not shift["is_source_for_rate"]:
            if avg_rate > 0: shift["earnings"] = shift["hours"] * avg_rate
            else: shift["earnings"] = shift["row_total_source"] - shift["bonus_in_row"] - shift["advance"] - shift["deduct"]
        
        shift["earnings"] = round(shift["earnings"], 2)
        hours_earnings_sum += shift["earnings"]
        total_hours_sum += shift["hours"] # <--- ПЛЮСУЕМ ЧАСЫ

        if shift["deduct"] != 0: current_emp["deductions"].append({"date": shift["date"].split(" ")[0], "amount": shift["deduct"]})
        if shift["advance"] != 0:
            if not (current_emp["start_balance"] > 0 and abs(abs(shift["advance"]) - current_emp["start_balance"]) < 2.0):
                current_emp["advances"].append({"date": shift["date"].split(" ")[0], "amount": shift["advance"]})

    total_bonuses = sum(b["amount"] for b in current_emp["bonuses"])
    total_deductions = sum(d["amount"] for d in current_emp["deductions"]) 
    total_advances = sum(a["amount"] for a in current_emp["advances"])
    
    # <--- ЗАПИСЫВАЕМ ИТОГИ ПО ЧАСАМ ---
    current_emp["total_hours"] = round(total_hours_sum, 2)
    current_emp["total_shift_pay"] = round(hours_earnings_sum, 2)

    current_emp["total_accrued"] = round(hours_earnings_sum + total_bonuses, 2)
    current_emp["calc_payout"] = round(current_emp["total_accrued"] + total_deductions + total_advances, 2)
    employees.append(current_emp)
    return employees

# --- РОУТЫ ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request, username: str = Depends(get_current_username)):
    return templates.TemplateResponse("index.html", {"request": request})

@app.post("/upload")
async def process_files(files: List[UploadFile] = File(...), username: str = Depends(get_current_username)):
    all_employees = []
    try:
        for file in files:
            content = await file.read()
            emps = parse_single_file(content, file.filename)
            all_employees.extend(emps)
        return all_employees
    except Exception as e:
        error_text = traceback.format_exc()
        return JSONResponse(status_code=500, content={"error": str(e), "traceback": error_text})

@app.post("/print", response_class=HTMLResponse)
async def print_view(request: Request, username: str = Depends(get_current_username)):
    try:
        form_data = await request.json()
        return templates.TemplateResponse("print_view.html", {"request": request, "employees": form_data})
    except Exception as e:
        return HTMLResponse(content=f"<h1>Ошибка</h1><pre>{traceback.format_exc()}</pre>")