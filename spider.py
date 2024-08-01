import json
from datetime import datetime
import scrapy
import requests
import pytz
import pandas as pd

class FipeSpider(scrapy.Spider):
    name = 'Fipe'
    start_urls = ['https://veiculos.fipe.org.br']
    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'DNT': '1',
        'X-Requested-With': 'XMLHttpRequest',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, Gecko) Chrome/81.0.4044.138 Safari/537.36',
        'Content-Type': 'application/json; charset=UTF-8'
    }
    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'DEFAULT_REQUEST_HEADERS': headers,
        'DOWNLOAD_DELAY': 1.5,  # Delay de 3 segundos entre as requisições
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 10,  # Número de tentativas de retry para falhas
        'RETRY_HTTP_CODES': [429],  # Requisições que retornam 429 serão tentadas novamente
        'HTTPCACHE_ENABLED': False  # Desativar cache
    }

    def __init__(self, year: str, month: str, brand_name: str, **kwargs):
        super().__init__(**kwargs)
        self.year = int(year)
        self.month = int(month)
        self.brand_name = brand_name
        self.list_month = "janeiro fevereiro março abril maio junho julho agosto setembro outubro novembro dezembro".split()
        month_name = self.list_month[int(month) - 1]
        self.reference = f"{month_name}/{year} "
        self.data = []

    def parse(self, response):
        self.logger.info(f"Parsing start_urls: {self.start_urls}")
        yield scrapy.Request(url="https://veiculos.fipe.org.br/api/veiculos//ConsultarTabelaDeReferencia",
                             callback=self.ref_tables, method="POST")

    def ref_tables(self, response):
        self.logger.info(f"ref_tables response: {response.text}")
        ref_tables = json.loads(response.text)
        ref_tables = [ref for ref in ref_tables if ref["Mes"] == self.reference]
        if not ref_tables:
            self.logger.warning(f"No reference tables found for {self.reference}")
        for table in ref_tables:
            formdata = {"codigoTabelaReferencia": table["Codigo"],
                        "codigoTipoVeiculo": "2"}  # 2 for motorcycles

            yield scrapy.Request(url="https://veiculos.fipe.org.br/api/veiculos//ConsultarMarcas",
                                 callback=self.brands,
                                 method="POST", body=json.dumps(formdata),
                                 meta={"formdata": formdata.copy()})

    def brands(self, response):
        self.logger.info(f"brands response: {response.text}")
        brands_table = json.loads(response.text)
        formdata = response.meta["formdata"]
        brand_found = False
        for brand in brands_table:
            if brand["Label"].lower() == self.brand_name.lower():
                brand_found = True
                formdata["codigoMarca"] = brand["Value"]
                yield scrapy.Request(url="https://veiculos.fipe.org.br/api/veiculos//ConsultarModelos",
                                     callback=self.models,
                                     method="POST",
                                     body=json.dumps(formdata),
                                     meta={"formdata": formdata.copy()})
                break
        if not brand_found:
            self.logger.warning(f"Brand {self.brand_name} not found.")

    def models(self, response):
        self.logger.info(f"models response: {response.text}")
        models_table = json.loads(response.text)
        formdata = response.meta["formdata"]
        if not models_table["Modelos"]:
            self.logger.warning(f"No models found for brand {self.brand_name}")
        for model in models_table["Modelos"]:
            formdata["codigoModelo"] = model["Value"]
            yield scrapy.Request(url="https://veiculos.fipe.org.br/api/veiculos//ConsultarAnoModelo",
                                 callback=self.years,
                                 method="POST",
                                 body=json.dumps(formdata),
                                 meta={"formdata": formdata.copy()})

    def years(self, response):
        self.logger.info(f"years response: {response.text}")
        years_table = json.loads(response.text)
        formdata = response.meta["formdata"]
        if not years_table:
            self.logger.warning(f"No years found for model {formdata.get('codigoModelo')}")
        for ano in years_table:
            formdata["anoModelo"], formdata["codigoTipoCombustivel"] = ano["Value"].split("-")
            formdata["tipoVeiculo"] = "moto"
            formdata["tipoConsulta"] = "tradicional"
            formdata['data_consulta'] = datetime.now(pytz.timezone('UTC')).strftime("%Y-%m-%d %H:%M:%S")
            yield scrapy.Request(url="https://veiculos.fipe.org.br/api/veiculos//ConsultarValorComTodosParametros",
                                 callback=self.get_data,
                                 method="POST",
                                 body=json.dumps(formdata.copy()),
                                 meta={"formdata": formdata})

    @staticmethod
    def parse_reference_month(reference_month):
        list_month = "janeiro fevereiro março abril maio junho julho agosto setembro outubro novembro dezembro".split()
        month, _, year = reference_month.split()
        month = list_month.index(month) + 1
        reference_month = int(f"{year}{month}")
        return reference_month

    def parse_data(self, response):
        response_data = json.loads(response.text)
        data = dict()
        data["ano"] = self.year
        data["mes"] = self.month
        data["valor"] = float(response_data["Valor"].replace("R$ ", "").replace(".", "").replace(",", "."))
        data["marca"] = response_data["Marca"]
        data["modelo"] = response_data["Modelo"]
        data["ano_modelo"] = str(response_data["AnoModelo"])
        data["combustivel"] = response_data["Combustivel"]
        data["codigo_fipe"] = response_data["CodigoFipe"]
        data["mes_referencia"] = self.parse_reference_month(response_data["MesReferencia"])
        data["tipo_veiculo"] = response_data["TipoVeiculo"]
        data["sigla_combustivel"] = response_data["SiglaCombustivel"]
        data["data_consulta"] = response.meta["formdata"]["data_consulta"]
        return data

    def get_data(self, response):
        self.logger.info(f"get_data response: {response.text}")
        data = self.parse_data(response)
        self.data.append(data)
        if len(self.data) % 10 == 0:  # Save every 10 entries as a checkpoint
            self.export_to_excel()

    def export_to_excel(self):
        df = pd.DataFrame(self.data)
        df.to_excel("FIPE2.xlsx", index=False)

    def closed(self, reason):
        self.export_to_excel()
        self.logger.info("Exported all data to Excel successfully.")
