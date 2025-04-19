#!/usr/bin/env python3
import os
import sys
import json
import requests
import sqlite3
from urllib.parse import urlencode

from dotenv import load_dotenv

from google.oauth2 import service_account
# Импорт для работы с Google Sheets
from requests.exceptions import HTTPError as HttpRequestError
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from datetime import datetime, timedelta

# ===============================
# Загружаем переменные окружения из .env
# ===============================
load_dotenv()

# ===============================
# Конфигурация hh.ru (OAuth2) через ENV
# ===============================
EXCLUDED_EMPLOYEE_IDS = [
    3177, 999442, 9330017, 10871726, 3443127, 2657797, 6153907, 2156474, 1545374,
    2553761, 1498795, 1999994, 5481550, 9579070, 3112459, 10004751, 11842692,
    3089914, 10623824, 9860737, 4263964, 9623282, 2899434, 3094193, 5830512,
    57073, 11056965, 4174021, 10753971, 1729313, 2022372, 5388489, 1740, 10753971,
    5805688, 611692, 4263964, 4856020, 3390849, 5302705, 5547644, 10753971,
    11571595, 11124587, 10321769, 11695543, 4671816, 2732037, 4333013, 11807162,
    2800609, 11807162, 5193393, 1141344, 9330017, 5687059, 3315744
]

HH_CLIENT_ID = os.environ.get("HH_CLIENT_ID", "")
HH_CLIENT_SECRET = os.environ.get("HH_CLIENT_SECRET", "")
HH_REDIRECT_URI = os.environ.get("HH_REDIRECT_URI", "https://example.com/")
HH_AUTHORIZATION_URL = "https://hh.ru/oauth/authorize"
HH_TOKEN_URL = "https://api.hh.ru/token"  # Обмен кода на токен
STATE = "random_state_string"  # Можно использовать случайную строку для защиты

# ===============================
# Настройки парсинга вакансий
# ===============================
DATABASE_FILE = "vacancies.db"  # Файл базы SQLite
HH_API_URL = "https://api.hh.ru/vacancies"
PER_PAGE = 100  # Количество вакансий за запрос
KEYWORD = "AmoCRM"  # Ключевое слово для поиска

# ===============================
# Настройки Google Sheets
# ===============================
SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


# ===============================
# Функция авторизации hh.ru
# ===============================
def get_hh_token():
    """
    Получаем/обновляем токен через OAuth2.
    """
    token_file = "hh_token.json"
    # Если файл с токеном уже есть – читаем
    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            token_data = json.load(f)
        return token_data.get("access_token")

    # Формирование URL для авторизации
    params = {
        "response_type": "code",
        "client_id": HH_CLIENT_ID,
        "state": STATE,
        "redirect_uri": HH_REDIRECT_URI,
    }
    auth_url = HH_AUTHORIZATION_URL + "?" + urlencode(params)
    print("Откройте следующую ссылку в браузере для авторизации:")
    print(auth_url)
    print("После авторизации скопируйте значение параметра 'code' из URL и вставьте его ниже.")
    code = input("Введите authorization code: ").strip()

    # Обмен authorization_code на access_token
    data = {
        "grant_type": "authorization_code",
        "client_id": HH_CLIENT_ID,
        "client_secret": HH_CLIENT_SECRET,
        "code": code,
        "redirect_uri": HH_REDIRECT_URI,
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    response = requests.post(HH_TOKEN_URL, data=data, headers=headers)
    response.raise_for_status()
    token_data = response.json()
    with open(token_file, "w") as f:
        json.dump(token_data, f)
    print("Токен успешно получен и сохранён в", token_file)
    return token_data.get("access_token")

def refresh_hh_token(refresh_token):
    """
    Обновляет access и refresh токены HH.ru.
    
    :param refresh_token: Текущий refresh_token
    :param client_id: Client ID приложения HH.ru
    :param client_secret: Client Secret приложения HH.ru
    :return: Новый access_token или None при ошибке
    """
    token_url = "https://hh.ru/oauth/token"  # Официальный эндпоинт HH.ru
    
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": HH_CLIENT_ID,
        "client_secret": HH_CLIENT_SECRET
    }
    
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    try:
        response = requests.post(token_url, data=data, headers=headers)
        response.raise_for_status()
        
        token_data = response.json()
        
        # Сохраняем новые токены (включая новый refresh_token)
        with open("hh_token.json", "w") as f:
            json.dump(token_data, f)
            
        return token_data["access_token"]
        
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при обновлении токенов: {e}")
        return None
    
# ===============================
# Работа с базой данных
# ===============================
def init_db():
    """
    Создаем таблицу, если её нет.
    """
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hh_vacancies (
            id TEXT PRIMARY KEY,
            vacancy_name TEXT,
            url TEXT,
            employer_id TEXT,
            employer_name TEXT,
            city TEXT,
            contact_name TEXT,
            phones TEXT,
            email TEXT,
            prof_roles TEXT,
            industry TEXT,
            published_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def save_to_db(vacancies):
    """
    Сохранение массива вакансий в базу SQLite.
    """
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    for vac in vacancies:
        vac_id = vac.get("id", "")
        vacancy_name = vac.get("name", "")
        url = vac.get("alternate_url", "")

        # Инфа о компании
        employer = vac.get("employer", {})
        employer_id = employer.get("id", "")
        if employer_id and employer_id.isdigit():
            # Убедимся, что employer_id - число, чтобы сравнивать корректно
            if int(employer_id) in EXCLUDED_EMPLOYEE_IDS:
                continue
        employer_name = employer.get("name", "Не указано")
        industries = employer.get("industries", [])
        industry = industries[0].get("name", "") if len(industries) > 0 else ""

        # Город
        address = vac.get("address")
        if address and "city" in address and address["city"]:
            city = address["city"]
        else:
            area = vac.get("area", {})
            city = area.get("name", "Не указано")

        # Контакты
        contacts = vac.get("contacts")
        if contacts:
            contact_name = contacts.get("name", "")
            email = contacts.get("email", "")
            phones_arr = contacts.get("phones", [])
        else:
            contact_name = ""
            email = ""
            phones_arr = []

        # Собираем телефоны
        phone_strings = []
        has_contacts = False

        for phone_obj in phones_arr:
            country = phone_obj.get("country", "")
            city_code = phone_obj.get("city", "")
            number = phone_obj.get("number", "")
            comment = phone_obj.get("comment", "")
            if number:
                has_contacts = True
            phone_full = f"+{country} ({city_code}) {number}"
            if comment:
                phone_full += f" [{comment}]"
            phone_strings.append(phone_full)

        # Если нет email и нет ни одного телефона — пропускаем
        if not email and not has_contacts:
            continue

        phones = "\n".join(phone_strings)

        # Профессиональные роли
        prof_roles_arr = vac.get("professional_roles", [])
        prof_roles_names = [role.get("name", "") for role in prof_roles_arr]
        prof_roles = ", ".join(prof_roles_names)

        published_at = vac.get("published_at", "")

        # Запись в базу (INSERT OR IGNORE, чтобы не дублировать)
        cursor.execute("""
            INSERT OR IGNORE INTO hh_vacancies (
                id, vacancy_name, url,
                employer_id, employer_name, city,
                contact_name, phones, email,
                prof_roles, industry, published_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            vac_id, vacancy_name, url,
            employer_id, employer_name, city,
            contact_name, phones, email,
            prof_roles, industry, published_at
        ))

    conn.commit()
    conn.close()


# ===============================
# Парсинг всех вакансий (по 100 шт. на странице)
# ===============================
def parse_all_vacancies(access_token):
    """
    Последовательно проходим все страницы (до 200) и сохраняем вакансии.
    """
    page = 0
    per_page = 100
    total_saved = 0

    while True:
        params = {
            "text": KEYWORD,
            "per_page": per_page,
            "page": page
        }
        headers = {
            "Authorization": f"Bearer {access_token}"
        }
        response = requests.get(HH_API_URL, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        vacancies = data.get("items", [])
        if not vacancies:
            print("Больше вакансий нет или достигнут лимит. Останавливаемся.")
            break

        save_to_db(vacancies)
        total_saved += len(vacancies)
        print(f"Страница {page}. Получено {len(vacancies)} вакансий (итого сохранено {total_saved}).")

        page += 1
        if page >= data.get("pages", 1):
            print("Достигли последней доступной страницы, завершаем цикл.")
            break

        if page >= 200:
            print("Достигнут лимит 200 страниц API hh.ru (20 000 вакансий). Останавливаемся.")
            break

    print(f"Всего сохранено {total_saved} вакансий.")


# ===============================
# Парсинг по заданному диапазону дат
# ===============================
def parse_by_date_range(access_token, date_from, date_to):
    """
    Собирает все вакансии по ключевому слову за указанный диапазон дат.
    """
    page = 0
    per_page = 100
    total_saved = 0
    print(f"Начинаем сбор за период {date_from}..{date_to}")

    while True:
        params = {
            "text": KEYWORD,
            "per_page": per_page,
            "page": page,
            "date_from": date_from,
            "date_to": date_to,
            "excluded_employer_id": EXCLUDED_EMPLOYEE_IDS
        }
        headers = {
            "Authorization": f"Bearer {access_token}"
        }

        response = requests.get(HH_API_URL, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        vacancies = data.get("items", [])
        if not vacancies:
            print(f"Нет больше вакансий в интервале {date_from} .. {date_to}. Останавливаемся.")
            break

        save_to_db(vacancies)
        total_saved += len(vacancies)
        print(
            f"Диапазон {date_from}..{date_to}, страница {page}. Получено {len(vacancies)} вакансий (сумма {total_saved}).")

        page += 1
        if page >= data.get("pages", 1):
            print(f"Достигли последней страницы (диапазон {date_from}..{date_to}), завершаем цикл.")
            break

        if page >= 200:
            print("Достигнут лимит 200 страниц (20 000 вакансий) для данного диапазона.")
            break

    print(f"Итого сохранено {total_saved} вакансий за период {date_from}..{date_to}.")


# ===============================
# Разбивка периода на равные части
# ===============================
def parse_with_parts(access_token, date_from, date_to, parts):
    """
    Разбивает [date_from; date_to] на 'parts' равных отрезков (по дням)
    и вызывает parse_by_date_range для каждого отрезка.
    """
    fmt_date = "%Y-%m-%d"
    dt_start = datetime.strptime(date_from, fmt_date)
    dt_end = datetime.strptime(date_to, fmt_date)

    if dt_end < dt_start:
        print(f"Ошибка: date_end < date_start ({date_to} < {date_from})")
        return
    if parts <= 0:
        print(f"Ошибка: parts={parts}, должно быть > 0.")
        return

    total_days = (dt_end - dt_start).days + 1
    days_per_part = total_days // parts
    remainder = total_days % parts

    current_start = dt_start
    for i in range(parts):
        extra = 1 if i < remainder else 0
        current_end = current_start + timedelta(days=days_per_part + extra - 1)
        if current_end > dt_end:
            current_end = dt_end

        # Преобразуем в ISO с точностью до секунд
        range_start_str = current_start.strftime("%Y-%m-%dT00:00:00")
        range_end_str = current_end.strftime("%Y-%m-%dT23:59:59")

        print(f"== Отрезок {i + 1} из {parts}: {range_start_str}..{range_end_str} ==")
        parse_by_date_range(access_token, range_start_str, range_end_str)

        current_start = current_end + timedelta(days=1)
        if current_start > dt_end:
            break


# ===============================
# Новая функция:
# Парсинг за последние 3 месяца с разбивкой на 30 частей
# ===============================
def parse_last_1_months(access_token, parts=30):
    """
    Берём дату 'сейчас' и дату '3 месяца назад' (90 дней),
    затем парсим вакансии с разбиением на parts частей.
    """
    today = datetime.now()
    three_months_ago = today - timedelta(days=60)

    date_from = three_months_ago.strftime("%Y-%m-%d")
    date_to = today.strftime("%Y-%m-%d")

    parse_with_parts(access_token, date_from, date_to, parts)


# ===============================
# Авторизация Google Sheets
# ===============================
def get_google_creds():
    """
    Авторизация через OAuth (логин пользователя), сохраняем token_google.json.
    """
    token_file = "token_google.json"
    creds = None
    if os.path.exists(token_file):
        creds = Credentials.from_authorized_user_file(token_file, SCOPES)
    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file("credentials_google.json", SCOPES)
        creds = flow.run_local_server(port=0)
        with open(token_file, "w") as f:
            f.write(creds.to_json())
    return creds


def get_google_creds_service_account(cred_path="credentials_google.json"):
    """
    Авторизация через Service Account.
    """
    credentials = service_account.Credentials.from_service_account_file(
        cred_path, scopes=SCOPES
    )
    return credentials


# ===============================
# Экспорт данных в Google Sheets
# ===============================
def export_to_google_sheets():
    """
    Выгружает данные из БД в указанный Google Sheet.
    """
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()

    # Базовый SQL-запрос
    query = """
        SELECT
            id, vacancy_name, city, employer_id, employer_name,
            contact_name, COALESCE(phones, ''), email,
            prof_roles, industry, published_at, url
        FROM hh_vacancies
    """

    # Исключаем определённых работодателей
    if EXCLUDED_EMPLOYEE_IDS:
        placeholders = ",".join(["?"] * len(EXCLUDED_EMPLOYEE_IDS))
        query += f" WHERE employer_id NOT IN ({placeholders})"

    query += " ORDER BY published_at DESC"

    if EXCLUDED_EMPLOYEE_IDS:
        cursor.execute(query, [str(eid) for eid in EXCLUDED_EMPLOYEE_IDS])
    else:
        cursor.execute(query)

    rows = cursor.fetchall()
    conn.close()

    # Заголовки
    headers = [
        "ID", "Вакансия", "Город", "Employer ID", "Компания",
        "Контактное лицо", "Телефон(ы)", "Почта",
        "Проф. Роли", "Отрасль", "Дата публ.", "URL"
    ]
    data = [headers] + list(rows)

    # Авторизация и отправка в Google Sheets
    creds = get_google_creds_service_account("credentials_google.json")
    service = build("sheets", "v4", credentials=creds)
    sheet = service.spreadsheets()

    range_name = "Sheet!A1"  # При необходимости поменяйте название листа
    sheet.values().update(
        spreadsheetId=SHEET_ID,
        range=range_name,
        valueInputOption="RAW",
        body={"values": data}
    ).execute()

    print(f"Обновлено строк: {len(rows)}")

def get_refresh_token():
    token_file = "hh_token.json"
    # Если файл с токеном уже есть – читаем
    if os.path.exists(token_file):
        with open(token_file, "r") as f:
            token_data = json.load(f)
        return token_data.get("refresh_token")
    
# ===============================
# Главная функция
# ===============================
def main():
    init_db()
    access_token = get_hh_token()

    try:
        parse_last_1_months(access_token, parts=20)
    except HttpRequestError as e:
        print(e)
        refresh_hh_token(get_refresh_token())
        raise
        
    export_to_google_sheets()


if __name__ == "__main__":
    main()
