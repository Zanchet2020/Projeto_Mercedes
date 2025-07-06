import os
import argparse
import re
import camelot
import pandas as pd
import pdfplumber
from multiprocessing import Pool

os.environ["LD_PRELOAD"] = "/usr/lib/x86_64-linux-gnu/libstdc++.so.6"

class MetadataExtractor:
    def __init__(self, pdf_path):
        self.pdf_path = pdf_path
        self.text = self._extract_first_page()

    def _extract_first_page(self):
        with pdfplumber.open(self.pdf_path) as pdf:
            return pdf.pages[0].extract_text()

    def extract_city(self):
        match = re.search(r"Municipio de\s+(.+)", self.text, re.IGNORECASE)
        return match.group(1).strip().split(' ')[0] if match else None

    def extract_date(self):
        match = re.search(r"Período:\s*\d{2}/\d{2}/\d{4}\s+até:\s*(\d{2}/\d{2}/\d{4})", self.text)
        return match.group(1).strip() if match else None


class TableExtractor:
    def __init__(self, pdf_path):
        self.tables = camelot.read_pdf(pdf_path, pages="all", flavor="lattice")

    def get_tables(self):
        return [table.df for table in self.tables]


class DataProcessor:
    def __init__(self, tables, city, date):
        self.tables = tables
        self.city = city
        self.date = date

    def extract_entries(self):
        entries = [
            df for df in self.tables
            if df.shape[1] > 0 and df.iloc[0, 0].strip().lower() == "reduzido"
        ]
        cleaned = []
        for df in entries:
            df_cleaned = df[
                ~df.iloc[:, 0].str.strip().str.lower().eq("reduzido")
                & df.iloc[:, 1].notna()
                & df.iloc[:, 1].str.strip().ne("")
            ]
            cleaned.append(df_cleaned.reset_index(drop=True))
        combined = pd.concat(cleaned, ignore_index=True)
        combined.columns = entries[0].iloc[0]
        return combined

    def extract_sources(self):
        sources = [
            df for df in self.tables
            if df.shape[1] > 0 and df.iloc[0, 0].strip().lower() == "código"
        ]
        result = pd.DataFrame()
        for src in sources:
            codes = src.iloc[1:, 0].str.split('\n', expand=True).stack().reset_index(drop=True)
            description = src.iloc[1:, 1].str.split('\n', expand=True).stack().reset_index(drop=True)
            balance = src.iloc[1:, 2].str.split('\n', expand=True).stack().reset_index(drop=True)

            temp = pd.concat([codes, description, balance], axis=1)
            temp.columns = ["Código", "Descrição", "Saldo Atual"]
            result = pd.concat([result, temp], ignore_index=True)

        result.drop(result.tail(2).index, inplace=True)
        result.reset_index(drop=True, inplace=True)
        return result

    def merge(self, entries):
        final = pd.DataFrame(columns=[
            "Município", "Data", "Reduzido", "Conta", "Descrição da Conta",
            "Fonte", "Descrição da Fonte", "Saldo Atual"
        ])

        for i in range(0, len(entries) - 1, 2):
            even = entries.iloc[i]
            odd = entries.iloc[i + 1]
            odd_lines = str(odd.iloc[1]).split('\n')

            for j, line in enumerate(odd_lines):
                if pd.isna(line) or line.strip() == "":
                    continue
                new_row = {
                    "Município": self.city,
                    "Data": self.date,
                    "Reduzido": even.iloc[0],
                    "Conta": str(even.iloc[1]).split(' ')[0],
                    "Descrição da Conta": ' '.join(str(even.iloc[1]).split(' ')[1:]),
                    "Fonte": line.strip(),
                    "Descrição da Fonte": str(odd.iloc[2]).split('\n')[j].strip(),
                    "Saldo Atual": str(odd.iloc[7]).split('\n')[j].strip()
                }
                final = pd.concat([final, pd.DataFrame([new_row])], ignore_index=True)

        return final

def process_pdf(pdf_path, output_base, index):
    try:
        print(f"[{index}] Processando: {pdf_path}")
        meta = MetadataExtractor(pdf_path)
        city = meta.extract_city() or input(f"Cidade não encontrada em {pdf_path}. Informe manualmente: ").strip()
        date = meta.extract_date() or input(f"Data não encontrada em {pdf_path}. Informe manualmente (DD/MM/AAAA): ").strip()

        tables = TableExtractor(pdf_path).get_tables()
        processor = DataProcessor(tables, city, date)
        entries = processor.extract_entries()
        #sources = processor.extract_sources()
        final = processor.merge(entries)

        final_path = f"{output_base}_{index}.xlsx"
        source_path = f"{output_base}_sources_{index}.xlsx"
        final.to_excel(final_path, index=False)
        #sources.to_excel(source_path, index=False)

        return f"[{index}] Finalizado: {final_path}"
    except Exception as e:
        return f"[{index}] Erro: {pdf_path} - {e}"

def main():
    parser = argparse.ArgumentParser(description="Conversor de PDFs para Excel")
    parser.add_argument("-i", "--input", nargs="+", required=True, help="Arquivos PDF de entrada")
    parser.add_argument("-o", "--output", required=True, help="Prefixo do arquivo de saída (sem extensão)")
    args = parser.parse_args()

    input_jobs = [(pdf, args.output, i) for i, pdf in enumerate(args.input, start=1)]

    with Pool() as pool:
        results = pool.starmap(process_pdf, input_jobs)

    for result in results:
        print(result)

if __name__ == "__main__":
    main()