import MetaTrader5 as mt5
from datetime import datetime
from dataclasses import dataclass
import pandas as pd


@dataclass
class EtlMt5:
    __usuario: int = 5023493635
    __password: str = 'PfQxZs*6'
    __servidor: str = 'MetaQuotes-Demo'

    def __conector(self) -> None:
        if not mt5.initialize():
            print(f"initialize() falló, codigo del error = {mt5.last_error()}")
            quit()

        sesion = mt5.login(login=self.__usuario, password=self.__password, server=self.__servidor)
        if sesion:
            print(f"Conectado a #{self.__usuario}")
        else:
            print(f"Conexión fallida a #{self.__usuario}, codigo del error: {mt5.last_error()}")

    @staticmethod
    def __desconector() -> None:
        mt5.shutdown()

    @staticmethod
    def __obtener_datos() -> tuple:
        fecha_inicial = datetime(2024, 1, 1)
        fecha_final = datetime.now()
        ordenes = mt5.history_deals_get(fecha_inicial, fecha_final)
        return ordenes

    @staticmethod
    def __limpiar_datos(tupla: tuple) -> dict[str, list]:
        datos_limpios: dict[str, list] = {
            'position_id': [],
            'time': []
        }
        for i in tupla:
            datos_limpios['position_id'].append(i[7])
            datos_limpios['time'].append(pd.to_datetime(i[2], unit='s'))

        return datos_limpios

    @staticmethod
    def __conv_dataframe(datos_) -> pd.DataFrame:
        df = pd.DataFrame(datos_)
        return df

    @staticmethod
    def __exportar(df: pd.DataFrame) -> None:
        nombre_archivo: str = 'datos.xlsx'
        df.to_excel(nombre_archivo)

    @staticmethod
    def a_minutos(tiempo_: str) -> int:
        dias, horas, minutos = int(tiempo_[0:2]), int(tiempo_[-8:-6]), int(tiempo_[-5:-3])
        resultado: int = (dias * 24 + horas) * 60 + minutos
        return resultado

    def main(self) -> None:
        self.__conector()
        data = self.__conv_dataframe(self.__limpiar_datos(self.__obtener_datos()))
        data = data.groupby(['position_id'])['time'].diff().dropna()
        data = list(dict(data).values())
        diccionario_final = {
            'tiempo': []
        }
        for tiempo in data:
            tiempo = self.a_minutos(str(tiempo))
            diccionario_final['tiempo'].append(tiempo)
        dataframe_final = self.__conv_dataframe(diccionario_final)
        self.__exportar(dataframe_final)


if __name__ == '__main__':
    on: EtlMt5 = EtlMt5()
    on.main()
