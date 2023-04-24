from aiogram import Bot, Dispatcher, types
import asyncio
from config import BOT_TOKEN, api_key, api_secret
import os
import csv
import pandas as pd
import numpy as np
from binance.client import Client
import talib
import time


# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
chat_id = ''

symbols = ['BTCUSDT', 'ETHUSDT']
interval = Client.KLINE_INTERVAL_5MINUTE
csv_files = [s+'_candles_5min.csv' for s in symbols]


async def update_candles():
    client = Client(api_key, api_secret)
    current_time = client.get_server_time()['serverTime']
    delta_time = 7 * 24 * 60 * 60 * 1000
    start_time = current_time - delta_time

    head_s = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time', 'Quote asset volume', 'Number of trades',
              'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
    df = {}
    last_recorded_close_time = 0
    for i in range(len(csv_files)):
        # Сохраняем последние 30 дней
        if os.path.exists(csv_files[i]) and os.path.getsize(csv_files[i]) > 0:
            df[i] = pd.read_csv(csv_files[i])
            df[i]['Open time'] = pd.to_datetime(df[i]['Open time'], unit='ms')

            last_timestamp = int(df[i]['Open time'].tail(1).values[0].astype(int) / 1e6)
            time_difference = current_time - last_timestamp

            if time_difference < delta_time:
                new_start_time = last_timestamp + 5 * 60 * 100
                new_candles = client.get_historical_klines(symbols[i], interval, new_start_time, current_time - 5*60*1000)
                with open(csv_files[i], 'a', newline='') as f:
                    writer = csv.writer(f)
                    for candle in new_candles:
                        writer.writerow(candle)
            else:
                candles = client.get_historical_klines(symbols[i], interval, start_time,  current_time - 5*60*1000)
                with open(csv_files[i], 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(head_s)
                    for candle in candles:
                        writer.writerow(candle)
        else:
            candles = client.get_historical_klines(symbols[i], interval, start_time,  current_time - 5*60*1000)
            with open(csv_files[i], 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(head_s)
                for candle in candles:
                    writer.writerow(candle)
        df[i] = pd.read_csv(csv_files[i])
        df[i]['Open time'] = pd.to_datetime(df[i]['Open time'], unit='ms')
        last_recorded_close_time = int(df[i]['Close time'].tail(1).values[0].astype(int))
        print(csv_files[i] + ' updated')
        # print(df[i])


    # Каждые 5 минут дочитываем свечки
    while True:
        new_last_time = last_recorded_close_time
        for i in range(len(symbols)):
            current_time = client.get_server_time()['serverTime']
            if current_time - last_recorded_close_time > 5 * 60 * 1000 + 1:
                print(f'add 1 candle for {symbols[i]}: ')
                last_candle = client.get_klines(symbol=symbols[i], interval=interval, startTime=last_recorded_close_time + 1, limit=1)
                with open(csv_files[i], 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(last_candle[0])

                new_last_time = last_candle[0][6]
                lc_d = {'Open time': [pd.to_datetime(last_candle[0][0], unit='ms')], 'Open': [float(last_candle[0][1])],
                        'High': [float(last_candle[0][2])], 'Low': [float(last_candle[0][3])], 'Close': [float(last_candle[0][4])],
                        'Volume': [float(last_candle[0][5])], 'Close time': [pd.to_datetime(last_candle[0][6], unit='ms')],
                        'Quote asset volume': [float(last_candle[0][7])], 'Number of trades': [int(last_candle[0][8])],
                        'Taker buy base asset volume': [float(last_candle[0][9])],
                        'Taker buy quote asset volume': [float(last_candle[0][10])],
                        'Ignore': [int(last_candle[0][11])]}
                last_candle_df = pd.DataFrame(lc_d)
                print(lc_d)
                df[i] = pd.concat([df[i], last_candle_df], ignore_index=True)

            signals = []

            # Вычисление скользящих средних и дополнительных показателей
            close_prices = np.array(df[i]['Close'].astype(float))
            sma = {50: talib.SMA(close_prices, timeperiod=50), 200: talib.SMA(close_prices, timeperiod=200),
                   500: talib.SMA(close_prices, timeperiod=500), 1000: talib.SMA(close_prices, timeperiod=1000)}
            # Обнаружение сигналов: проверка на пересечение скользящих средних
            for key1, value1 in sma.items():
                for key2, value2 in sma.items():
                    if key1 < key2:
                        if value1[-1] > value2[-1] and value1[-2] <= value2[-2]:
                            signals.append(f"SMA {key1}-{key2}: Восходящий тренд")
                        if value1[-1] < value2[-1] and value1[-2] >= value2[-2]:
                            signals.append(f"SMA {key1}-{key2}: Нисходящий тренд")

            # Bollinger Bands
            upper, middle, lower = talib.BBANDS(close_prices, timeperiod=20)
            # Проверка на поджатие к уровням Bollinger Bands
            if close_prices[-1] >= upper[-1] or close_prices[-1] <= lower[-1]:
                signals.append(f"Bollinger Bands: Поджатие к уровню: ${close_prices[-1]:.2f}")

            # RSI
            rsi = talib.RSI(close_prices, timeperiod=14)
            # Проверка на перекупленность и перепроданность по RSI
            if rsi[-1] >= 70:
                signals.append("RSI: Перекупленность")
            if rsi[-1] <= 30:
                signals.append("RSI: Перепроданность")

            # MACD
            macd, macd_signal, _ = talib.MACD(close_prices, fastperiod=12, slowperiod=26, signalperiod=9)
            # Проверка на сигналы по MACD
            if macd[-1] > macd_signal[-1] and macd[-2] <= macd_signal[-2]:
                signals.append("MACD: Бычий сигнал")
            if macd[-1] < macd_signal[-1] and macd[-2] >= macd_signal[-2]:
                signals.append("MACD: Медвежий сигнал")

            # Объем торгов
            volume = np.array(df[i]['Volume'])
            # Проверка на повышенный объем торгов
            avg_volume = np.mean(volume[-20:])
            if volume[-1] > avg_volume * 1.5:
                signals.append("Высокий объем торгов")

            # Проверка и вывод сигналов
            if signals:
                print(f"Сигналы для {symbols[i]} на {df[i]['Open time'].iloc[-1]}:")
                s = f"Сигналы для {symbols[i]} на {df[i]['Open time'].iloc[-1]}:"
                for signal in signals:
                    print(signal)
                    s+='\n'+signal
                await send_signal(text=s)
            else:
                print(f"Сигналов для {symbols[i]} на {df[i]['Open time'].iloc[-1]} не обнаружено")
                # await send_signal(text=f"Сигналов для {symbols[i]} на {df[i]['Open time'].iloc[-1]} не обнаружено")
            # print(df)
        last_recorded_close_time = new_last_time
        current_time = client.get_server_time()['serverTime']
        time_until_next_candle = last_recorded_close_time + 5 * 60 * 1000 + 1 - current_time
        await asyncio.sleep(time_until_next_candle / 1000)


# Обработчик команды /start
async def cmd_start(message: types.Message):
    global chat_id
    chat_id = message.chat.id
    await update_candles()
    await message.reply("Привет! Я твой асинхронный Telegram-бот.")

# Обработчик эхо-сообщений
async def echo_message(message: types.Message):
    text = message.text
    await message.reply(f"Вы сказали: {text}")

# Сообщение о сигналах
async def send_signal(text):
    await bot.send_message(chat_id=chat_id, text=text)


# Обработчик команд
@dp.message_handler(commands=["start"])
async def handle_cmd(message: types.Message):
    await cmd_start(message)

# Обработчик текстовых сообщений
@dp.message_handler(content_types=types.ContentTypes.TEXT)
async def handle_text(message: types.Message):
    await echo_message(message)

if __name__ == "__main__":
    from aiogram import executor
    executor.start_polling(dp, skip_updates=True)
