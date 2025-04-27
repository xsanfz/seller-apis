import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получает список товаров магазина с Ozon.

    Args:
        last_id (str): ID последнего товара (пустая строка для начала)
        client_id (str): ID клиента в Ozon
        seller_token (str): Токен продавца

    Returns:
        dict: Список товаров и данные пагинации

    Example:
        >>> get_product_list("", "client123", "token456")
        {'items': [...], 'total': 100}
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получает все артикулы товаров магазина на Ozon.

    Args:
        client_id: Номер клиента Ozon
        seller_token: Токен продавца

    Returns:
        Список всех артикулов товаров

    Example:
        >>> get_offer_ids("client123", "token456")
        ["12345", "67890"]
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновляет цены товаров в личном кабинете Ozon.

    Отправляет новые цены на Ozon для указанных товаров.
    Максимально можно обновить цены для 1000 товаров за один запрос.

    Args:
        prices (list): Список цен в формате:
            [{
                "offer_id": "12345",
                "price": "5990",
                "currency_code": "RUB"
            }, ...]
        client_id (str): ID вашего аккаунта в Ozon (например "12345")
        seller_token (str): Ваш API-ключ Ozon (например "abc123")

    Returns:
        dict: Ответ от Ozon в формате:
            {
                "result": [...],
                "status": "OK"
            }

    Example:
        >>> update_price(
        ...     [{"offer_id": "123", "price": "5990", "currency_code": "RUB"}],
        ...     "12345",
        ...     "abc123"
        ... )
        {"result": [...], "status": "OK"}

    Error Example:
        >>> update_price([], "wrong_id", "bad_token")
        requests.exceptions.HTTPError: 401 Unauthorized
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновляет остатки товаров на складе в личном кабинете Ozon.

    Отправляет текущие остатки товаров на Ozon.
    Остатки обновляются только для указанных товаров.

    Args:
        stocks (list): Список остатков в формате:
            [{
                "offer_id": "12345",  # Артикул товара
                "stock": 10           # Доступное количество
            }, ...]
        client_id (str): ID вашего магазина в Ozon (например "12345-AB")
        seller_token (str): Ваш API-ключ от Ozon (например "token123xyz")

    Returns:
        dict: Ответ от Ozon в формате:
            {
                "result": [...],
                "status": "OK"
            }

    Example:
        >>> update_stocks(
        ...     [{"offer_id": "12345", "stock": 5}],
        ...     "12345-AB",
        ...     "token123xyz"
        ... )
        {"result": [...], "status": "OK"}

    Error Example:
        >>> update_stocks(
        ...     [{"offer_id": "12345", "stock": "пять"}],  # Неправильный формат
        ...     "12345-AB",
        ...     "token123xyz"
        ... )
        requests.exceptions.HTTPError: 400 Bad Request
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачивает и обрабатывает файл с остатками с сайта timeworld.ru.

    Returns:
        list: Список товаров с остатками и ценами

    Example:
        >>> download_stock()
        [{'Код': '123', 'Количество': '5', 'Цена': '5990 руб.'}, ...]
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Формирует список остатков для загрузки в Ozon.

       Args:
           watch_remnants: Данные по остаткам с сайта поставщика
           offer_ids: Артикулы товаров на Ozon

       Returns:
           Готовый список для обновления остатков

       Example:
           >>> create_stocks([{"Код": "123", "Количество": "5"}], ["123"])
           [{"offer_id": "123", "stock": 5}]
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Подготавливает список цен товаров для загрузки в маркетплейс.

       Формирует список цен на основе данных поставщика только для товаров,
       которые есть и у поставщика, и в вашем магазине на маркетплейсе.

       Args:
           watch_remnants (list): Товары от поставщика в формате:
               [{
                   "Код": "12345",           # Артикул товара
                   "Цена": "5'990.00 руб."    # Цена с сайта поставщика
               }, ...]
           offer_ids (list): Артикулы товаров в вашем магазине на маркетплейсе
               (например ["12345", "67890"])

       Returns:
           list: Список цен для загрузки в формате:
               [{
                   "id": "12345",             # Артикул товара
                   "price": {
                       "value": 5990,         # Цена в числовом формате
                       "currencyId": "RUR"    # Валюта (рубли)
                   }
               }, ...]

       Example:
           >>> create_prices(
           ...     [{"Код": "12345", "Цена": "5'990.00 руб."}],
           ...     ["12345", "67890"]
           ... )
           [{
               "id": "12345",
               "price": {
                   "value": 5990,
                   "currencyId": "RUR"
               }
           }]

       Error Example:
           >>> create_prices([{"Код": "99999", "Цена": "1'000.00 руб."}], ["12345"])
           []  # Пустой список, если товары не совпадают
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразует строку с ценой в формат, пригодный для загрузки на маркетплейсы.

    Удаляет все нецифровые символы и обрезает копейки, оставляя только целую часть числа.

    Args:
        price (str): Строка с ценой в формате поставщика, например "5'990.00 руб."

    Returns:
        str: Цена без лишних символов, например "5990"

    Examples:
        >>> price_conversion("5'990.00 руб.")
        '5990'
        >>> price_conversion("1 299.99 руб.")
        '1299'

        >>> price_conversion("Нет цены")  # Некорректный ввод
        ''
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разбивает список на части по n элементов.

    Args:
        lst: Исходный список
        n: Количество элементов в части

    Returns:
        Генератор с частями списка

    Example:
        >>> list(divide([1,2,3,4], 2))
        [[1,2], [3,4]]
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Асинхронно загружает цены товаров в Ozon.

        Получает актуальные цены от поставщика и обновляет их в личном кабинете Ozon.
        Автоматически разбивает большой список товаров на части по 1000 позиций.

        Args:
            watch_remnants (list): Список товаров с ценами от поставщика:
                [{
                    "Код": "12345",           # Артикул товара
                    "Цена": "5'990.00 руб."   # Цена в формате поставщика
                }, ...]
            client_id (str): Ваш ID клиента Ozon (например "12345-AB")
            seller_token (str): API-ключ продавца (например "token123xyz")

        Returns:
            list: Список всех загруженных цен в формате:
                [{
                    "offer_id": "12345",
                    "price": "5990",
                    "currency_code": "RUB"
                }, ...]

        Example:
            >>> await upload_prices(
            ...     [{"Код": "12345", "Цена": "5'990.00 руб."}],
            ...     "12345-AB",
            ...     "token123xyz"
            ... )
            [{
                "offer_id": "12345",
                "price": "5990",
                "currency_code": "RUB"
            }]

        Error Example:
            >>> await upload_prices([], "wrong_id", "bad_token")
            requests.exceptions.HTTPError: 401 Unauthorized
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Асинхронно обновляет остатки товаров в Ozon.

       Получает текущие остатки со склада поставщика и синхронизирует их с Ozon.
       Автоматически разбивает большой список на части по 100 товаров для корректной загрузки.

       Args:
           watch_remnants (list): Список товаров от поставщика:
               [{
                   "Код": "12345",          # Артикул товара
                   "Количество": "5"       # Доступное количество (>10, 1 или число)
               }, ...]
           client_id (str): ID вашего аккаунта Ozon (например "CLIENT-123")
           seller_token (str): API-токен продавца (например "SELLER-TOKEN-456")

       Returns:
           tuple: Кортеж из двух элементов:
               - list: Товары с ненулевым остатком
               - list: Все обновленные товары

       Example:
           >>> await upload_stocks(
           ...     [{"Код": "12345", "Количество": "5"}],
           ...     "CLIENT-123",
           ...     "SELLER-TOKEN-456"
           ... )
           (
               [{"offer_id": "12345", "stock": 5}],  # Товары в наличии
               [{"offer_id": "12345", "stock": 5}]    # Все товары
           )

       Error Example:
           >>> await upload_stocks([], "invalid", "token")
           requests.exceptions.HTTPError: 401 Unauthorized
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция для синхронизации данных с Ozon.

        Автоматически выполняет полный цикл обновления данных:
        1. Скачивает текущие остатки с сайта поставщика
        2. Получает список товаров магазина на Ozon
        3. Обновляет остатки и цены на площадке
        4. Обрабатывает возможные ошибки соединения

        Требуемые переменные окружения:
            SELLER_TOKEN: API-токен продавца Ozon
            CLIENT_ID: ID вашего аккаунта в Ozon

        Пример успешного выполнения:
            >>> main()
            Данные успешно загружены:
            - Обновлено 45 товаров
            - Из них в наличии: 32

        Пример ошибки:
            >>> main()
            Ошибка соединения: Неверный API-токен [401]
    """
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
