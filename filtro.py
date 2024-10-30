import os
import glob
import pandas as pd
import unicodedata

base_dir = os.path.dirname(os.path.abspath(__file__))

input_dir = os.path.join(base_dir, 'Baixados')

output_dir = os.path.join(base_dir, 'Filtradas')

def normalize_column_names(columns):
    normalized_columns = []
    for col in columns:
        col_normalized = unicodedata.normalize('NFKD', col).encode('ascii', 'ignore').decode('utf-8')
        normalized_columns.append(col_normalized)
    return normalized_columns

def get_latest_csv(input_dir):
    csv_files = glob.glob(os.path.join(input_dir, '*.CSV')) + glob.glob(os.path.join(input_dir, '*.csv'))
    if not csv_files:
        raise FileNotFoundError("Nenhum arquivo CSV encontrado na pasta.")
    return max(csv_files, key=os.path.getmtime)

def process_csv_file(file_path):
    print(f"Abrindo o arquivo CSV mais recente: {file_path}")

    df = pd.read_csv(file_path, delimiter=';', encoding='latin1')

    df.columns = normalize_column_names(df.columns)

    print("Colunas encontradas:", df.columns.tolist())

    column_map = {
        'NOME USUARIO': 'NOME USUARIO',
        'USUARIO': 'USUARIO',
        'DATA': 'DATA',
        'ESTACAO': 'ESTAAAO',
    }

    for col, csv_col in column_map.items():
        if csv_col not in df.columns:
            raise KeyError(f"A coluna '{csv_col}' não foi encontrada no arquivo.")
    df[column_map['NOME USUARIO']] = df[column_map['NOME USUARIO']].fillna(df[column_map['USUARIO']])

    df[column_map['USUARIO']] = df[column_map['USUARIO']].astype(str)
    df[column_map['NOME USUARIO']] = df[column_map['NOME USUARIO']].astype(str)
    df[column_map['ESTACAO']] = df[column_map['ESTACAO']].astype(str)

    filtered_df = pd.DataFrame(columns=['ESTACAO', 'USUARIO', 'NOME USUARIO'] + [f"{hour:02}:00-{hour:02}:59" for hour in range(24)])

    unique_combinations = df[[column_map['USUARIO'], column_map['NOME USUARIO'], column_map['ESTACAO']]].drop_duplicates().reset_index(drop=True)

    filtered_df['ESTACAO'] = unique_combinations[column_map['ESTACAO']]
    filtered_df['USUARIO'] = unique_combinations[column_map['USUARIO']]
    filtered_df['NOME USUARIO'] = unique_combinations[column_map['NOME USUARIO']]

    df[column_map['DATA']] = pd.to_datetime(df[column_map['DATA']], errors='coerce')

    for hour in range(24):
        hour_mask = (df[column_map['DATA']].dt.hour == hour)
        count_by_combination = df[hour_mask].groupby([column_map['USUARIO'], column_map['NOME USUARIO'], column_map['ESTACAO']]).size()

        for idx, (estacao, usuario, nome_usuario) in enumerate(zip(filtered_df['ESTACAO'], filtered_df['USUARIO'], filtered_df['NOME USUARIO'])):
            count = count_by_combination.get((usuario, nome_usuario, estacao), 0)
            filtered_df.loc[idx, f"{hour:02}:00-{hour:02}:59"] = count

    for hour in range(24):
        hour_column = f"{hour:02}:00-{hour:02}:59"
        filtered_df[hour_column] = pd.to_numeric(filtered_df[hour_column], errors='coerce')
        filtered_df[hour_column] = filtered_df[hour_column].replace(0, pd.NA)

    try:
        filtered_df['ESTACAO'] = pd.to_numeric(filtered_df['ESTACAO'])
    except ValueError:
        pass  

    try:
        filtered_df['USUARIO'] = pd.to_numeric(filtered_df['USUARIO'])
    except ValueError:
        pass  

    date_str = pd.Timestamp.now().strftime('%d-%m-%Y')
    output_file_name = f'Filtrada_{date_str}.xlsx'
    output_file_path = os.path.join(output_dir, output_file_name)

    with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
        filtered_df.to_excel(writer, sheet_name='Filtrada', index=False)

    print(f"Arquivo processado salvo em: {output_file_path}")

def main():
    latest_csv_file = get_latest_csv(input_dir)
    process_csv_file(latest_csv_file)

if __name__ == '__main__':
    main()
    