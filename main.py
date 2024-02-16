import argparse
import csv
import datetime
import json
import logging
import os
import psycopg2
import sqlite3

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

# Definição das variáveis de ambiente
db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

csv_file_path = os.getenv("CSV_FILE_PATH")


class DataSaver:
    def __init__(self):
        self.steps = [
            {
                'step': '1',
                'name': 'Extrair dados do banco de dados e do arquivo CSV',
                'description': 'Este passo extrai dados do banco de dados PostgreSQL e do arquivo CSV.',
                'function': self.extract_data_from_sources,
                'additional_steps': []
            },
            {
                'step': '2',
                'name': 'Escrever dados no disco',
                'description': 'Este passo escreve os dados extraídos em arquivos JSON no disco.',
                'function': self.write_datas_to_disk,
                'additional_steps': []
            },
            {
                'step': '3',
                'name': 'Executar consulta SQL',
                'description': 'Este passo executa uma consulta SQL nas tabelas de pedidos e detalhes de pedidos.',
                'function': self.query_orders,
                'additional_steps': []
            }
        ]
        self.db_data = {}
        self.csv_data = []
        self.local_data = []
        self.current_working_date = None

    def load_saved_data_to_memory(self, date=""):
        try:
            for db_folder in os.listdir('./data'):
                if os.path.isdir(os.path.join('./data', db_folder)):
                    for date_folder_or_table_folder in os.listdir(os.path.join('./data', db_folder)):
                        if os.path.isdir(os.path.join('./data', db_folder, date_folder_or_table_folder)):
                            self.current_working_date = date_folder_or_table_folder
                            if date != "" and date == date_folder_or_table_folder:
                                for day_folder in os.listdir(
                                        os.path.join('./data/', db_folder, date_folder_or_table_folder)):
                                    self.local_data.append(
                                        os.path.join('./data/', db_folder, date_folder_or_table_folder, day_folder))
                            for day_folder in os.listdir(os.path.join('./data/', db_folder, date_folder_or_table_folder)):
                                if date == "":
                                    self.local_data.append(
                                        os.path.join('./data/', db_folder, date_folder_or_table_folder, day_folder))
                        else:
                            for day in os.listdir('./data/' + db_folder + '/' + date_folder_or_table_folder):
                                self.current_working_date = day
                                if date != "" and date == day:
                                    for day_folder in os.listdir(
                                            os.path.join('./data/', db_folder, date_folder_or_table_folder, day)):
                                        self.local_data.append(
                                            os.path.join('./data/', db_folder, date_folder_or_table_folder, day,
                                                         day_folder))
                                for day_folder in os.listdir(
                                        os.path.join('./data/', db_folder, date_folder_or_table_folder, day)):
                                    if date == "":
                                        self.local_data.append(
                                            os.path.join('./data/', db_folder, date_folder_or_table_folder, day,
                                                         day_folder))
            return True, self.local_data
        except Exception as e:
            logging.error(e)
            return False, None

    def write_data_to_file(self, category, data, table_name=""):
        try:
            # Cria a estrutura de diretórios se ela não existir
            now = datetime.datetime.now()
            formatted_date = now.strftime("%Y-%m-%d")
            table_dir = os.path.join("./data", category, table_name, formatted_date)
            os.makedirs(table_dir, exist_ok=True)

            # Constrói o caminho do arquivo
            file_path = os.path.join(table_dir, table_name + ".json")  # Personalize o formato
            if category == "csv":
                file_path = os.path.join(table_dir, "order_details.json")

            # Converte objetos datetime.date para strings formatadas
            formatted_data = data
            if table_name != '':
                formatted_data = []
                for row in data:
                    formatted_row = {}
                    for inner_data in row:
                        row_value = row[inner_data]

                        if isinstance(row_value, datetime.date):
                            formatted_row[inner_data] = row_value.strftime("%Y-%m-%d")
                        elif '<memory' in str(row_value):  # Manipula dados binários
                            formatted_row[
                                inner_data] = "Sem imagem válida" if row_value.tobytes() == b'' else row_value.tobytes()
                        else:
                            formatted_row[inner_data] = row_value

                    formatted_data.append(formatted_row)

            # Salva os dados como JSON
            with open(file_path, "w") as json_file:
                json.dump(formatted_data, json_file, indent=4)
            return True
        except Exception as e:
            logging.error(e)
            return False

    def write_datas_to_disk(self):
        try:
            csv_result = self.write_data_to_file("csv", self.csv_data)
            for key, value in self.db_data.items():
                self.write_data_to_file("postgres", value, table_name=key)
            db_result = True
        except Exception as e:
            logging.error(e)
            db_result = False
            csv_result = False

        if db_result and csv_result:
            return True
        else:
            return False

    def extract_data_from_sources(self):
        # Obtém os dados do banco de dados
        result_db, self.db_data = self.db.fetch_and_save_all_data()

        # Obtém os dados do CSV
        self.csv_data = self.csv.save_csv_data()

        if self.csv_data and result_db:
            return True
        else:
            return False

    # Utilitário para encontrar etapas
    def find_step(self, number):
        for step in self.steps:
            if step['step'] == str(number):
                return step
        return None

    def run_step(self, step):
        # Isso executa a função salva na etapa.
        function_result = step['function']()

        # O texto do resultado depende do function_result
        result = f'A etapa {step["step"]} foi concluída com sucesso' if function_result else f'A etapa {step["step"]} teve um erro.'
        logging.info(f"\nEtapa: {step['step']}/{self.steps[-1]['step']}\n"
                     f" Nome: {step['name']}\n"
                     f"  Descrição: {step['description']}\n\n"
                     f" Resultado: {result}\n")

        # Se a etapa contiver etapas adicionais, execute-as
        if step['additional_steps']:
            for additional_step in step['additional_steps']:
                self.run_step(self.find_step(additional_step))

    def run_steps(self, steps):
        for step in steps:
            self.run_step(step)

    def query_orders(self):
        try:
            conn = sqlite3.connect(f'./merged_databases/merged_database_date-{self.current_working_date}.db')
            cur = conn.cursor()
            cur.execute("""
                        SELECT * FROM Orders;
                        """)
            orders = cur.fetchall()

            cur.execute("""
                        SELECT * FROM OrderDetails;
                        """)
            order_details = cur.fetchall()

            conn.close()

            # Juntando pedidos e detalhes de pedidos
            orders_with_details = []
            for order in orders:
                order_with_details = {'OrderID': order[0], 'CustomerID': order[1], 'OrderDate': order[2],
                                      'OrderDetails': []}
                for detail in order_details:
                    if detail[0] == order[0]:
                        order_with_details['OrderDetails'].append({'ProductID': detail[1],
                                                                    'Quantity': detail[2], 'UnitPrice': detail[3]})
                orders_with_details.append(order_with_details)

            # Salvar o resultado da consulta em um arquivo
            now = datetime.datetime.now()
            formatted_date = now.strftime("%Y-%m-%d")
            output_file_path = f"./query_output/query_{formatted_date}_gerado_em_{self.current_working_date.replace('-', '_')}.json"

            with open(output_file_path, 'w') as output_file:
                json.dump(orders_with_details, output_file, indent=4)

            logging.info(f"Resultado da consulta salvo em {output_file_path}")

            return True
        except Exception as e:
            logging.error(e)
            return False


class DbInput:
    def __init__(self, data_saver):
        self.data_saver = data_saver
        self.conn = None

    def connect_to_db(self):
        try:
            # Conectar ao banco de dados PostgreSQL
            self.conn = psycopg2.connect(
                dbname=db_params["dbname"],
                user=db_params["user"],
                password=db_params["password"],
                host=db_params["host"],
                port=db_params["port"]
            )
            logging.info("Conexão ao banco de dados PostgreSQL realizada com sucesso")
            return True
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            return False

    def fetch_and_save_all_data(self):
        if self.connect_to_db():
            try:
                cur = self.conn.cursor()

                # Buscar dados dos pedidos
                cur.execute("""
                            SELECT * FROM Orders;
                            """)
                orders = cur.fetchall()

                # Buscar dados dos detalhes do pedido
                cur.execute("""
                            SELECT * FROM OrderDetails;
                            """)
                order_details = cur.fetchall()

                self.data_saver.db_data['Orders'] = orders
                self.data_saver.db_data['OrderDetails'] = order_details

                return True, self.data_saver.db_data

            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(error)
                return False, None
            finally:
                if self.conn is not None:
                    self.conn.close()
                    logging.info("Conexão com o banco de dados fechada")
        else:
            return False, None


class CsvInput:
    def __init__(self, data_saver):
        self.data_saver = data_saver

    def save_csv_data(self):
        try:
            csv_data = []

            with open(csv_file_path, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    csv_data.append(row)

            self.data_saver.csv_data = csv_data

            return csv_data

        except Exception as e:
            logging.error(e)
            return None


def main():
    parser = argparse.ArgumentParser(description="Processar dados do PostgreSQL e CSV")
    parser.add_argument('--date', dest='date', action='store', default='',
                        help='Especifique opcionalmente a data para processamento (AAAA-MM-DD)')
    args = parser.parse_args()

    data_saver = DataSaver()

    # Se uma data específica for fornecida, processe os dados apenas para essa data
    if args.date:
        logging.info(f"Processando dados para a data: {args.date}")
        data_saver.load_saved_data_to_memory(args.date)
        data_saver.run_steps(data_saver.steps)
    else:
        # Caso contrário, processe todos os dados disponíveis
        data_saver.run_steps(data_saver.steps)


if __name__ == "__main__":
    main()
