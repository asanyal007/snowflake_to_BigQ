import pandas as pd
import datetime
import numpy as np
class audit:
    def create_audit(self,tables_columns):

        df_schema = pd.DataFrame(tables_columns, columns=['Schema', 'Database', 'tables'])
        df_schema.drop_duplicates(subset=None, keep='first', inplace=True)
        log_table = pd.DataFrame()
        log_table['dataset'] = df_schema['Schema']
        log_table['table'] = df_schema['tables']
        log_table['status'] = 'N'
        log_table['start'] = datetime.datetime.now()
        log_table['Finish'] = ''
        log_table['error'] = ''

        return log_table

    def update_audit(self,log_table, dataset, table, status, Finish, error):
        log_table['status'] = np.where((log_table['dataset']==dataset & log_table['table'] == table),status,
                                       log_table['status'])

        log_table['Finish'] = np.where((log_table['dataset'] == dataset & log_table['table'] == table), Finish,
                                       log_table['Finish'])

        log_table['error'] = np.where((log_table['dataset'] == dataset & log_table['table'] == table), error,
                                       log_table['error'])

        return log_table





