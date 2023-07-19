from env import get_db_url
import os
import pandas as pd
import numpy as np


################################### acquire zillow data ###################################




def acquire_zillow():
    '''
    This function checks to see if zillow.csv already exists, 
    if it does not, one is created
    '''
    #check to see if zillow.csv already exist
    if os.path.isfile('zillow.csv'):
        df = pd.read_csv('zillow.csv', index_col=0)
    
    else:
        
        url = get_db_url('zillow')
        df = pd.read_sql('''SELECT prop.*, 
                           pred.logerror, 
                           pred.transactiondate, 
                           air.airconditioningdesc, 
                           arch.architecturalstyledesc, 
                           build.buildingclassdesc, 
                           heat.heatingorsystemdesc, 
                           landuse.propertylandusedesc, 
                           story.storydesc, 
                           construct.typeconstructiondesc 

                    FROM   properties_2017 prop  
                           INNER JOIN (SELECT parcelid,
                                              logerror,
                                              Max(transactiondate) transactiondate 
                                       FROM   predictions_2017 
                                       GROUP  BY parcelid, logerror) pred
                                   USING (parcelid) 
                           LEFT JOIN airconditioningtype air USING (airconditioningtypeid) 
                           LEFT JOIN architecturalstyletype arch USING (architecturalstyletypeid) 
                           LEFT JOIN buildingclasstype build USING (buildingclasstypeid) 
                           LEFT JOIN heatingorsystemtype heat USING (heatingorsystemtypeid) 
                           LEFT JOIN propertylandusetype landuse USING (propertylandusetypeid) 
                           LEFT JOIN storytype story USING (storytypeid) 
                           LEFT JOIN typeconstructiontype construct USING (typeconstructiontypeid) 
                    WHERE  prop.latitude IS NOT NULL 
                           AND prop.longitude IS NOT NULL AND transactiondate <= '2017-12-31'
                            ;''', url)
        #creates new csv if one does not already exist
        df.to_csv('zillow.csv')


    return df



################################### nulls by column ###################################



def nulls_by_col():

    df = acquire_zillow()


    num_missing = df.isnull().sum() 
    rows = df.shape[0]
    prcnt_miss = num_missing / rows * 100
    cols_missing = pd.DataFrame({'num_rows_missing': num_missing, 'percent_rows_missing': prcnt_miss})
    return cols_missing




################################### nulls by row ###################################



def nulls_by_row():


    df = acquire_zillow()

    num_missing = df.isnull().sum(axis=1)
    prcnt_miss = num_missing / df.shape[1] * 100
    rows_missing = pd.DataFrame({'num_cols_missing': num_missing, 'percent_cols_missing': prcnt_miss})\
    .reset_index()\
    .groupby(['num_cols_missing', 'percent_cols_missing']).count()\
    .rename(index=str, columns={'customer_id': 'num_rows'}).reset_index()
    return rows_missing



################################### dropping multi unit residences ###################################



def clean_zillow():

    df = acquire_zillow()

    rows_missing = nulls_by_row(df)


    df = df[(df['propertylandusedesc'] != 'Duplex (2 Units, Any Combination)') & 
    (df['propertylandusedesc'] != 'Condominium') &
    (df['propertylandusedesc'] != 'Commercial/Office/Residential Mixed Used') & 
    (df['propertylandusedesc'] != 'Townhouse') & 
    (df['propertylandusedesc'] != 'Triplex (3 Units, Any Combination)')]

    cols_to_remove = ['id',
       'calculatedbathnbr', 'finishedsquarefeet12', 'fullbathcnt', 'heatingorsystemtypeid'
       ,'propertycountylandusecode', 'propertylandusetypeid','propertyzoningdesc', 
        'censustractandblock', 'propertylandusedesc', 'unitcnt']

    # dropping unneeded columns
    df = df.drop(columns=cols_to_remove)

    #drop columns with a null percentage greater than the col_threshold argument
    df = df.loc[:, df.isna().mean() < .4]
    #transpose the dataframe
    df = df.T
    #drop columns with a null percentage greater than the row_threshold argument
    df = df.loc[:, df.isna().mean() < .25]

    df = df.T

    df.drop(columns = 'heatingorsystemdesc', inplace = True)

    # should I fill missing values for buildingqualitytypeid with median value?
    df.buildingqualitytypeid.fillna(df.buildingqualitytypeid.median(), inplace = True)

    # fill missing values with median null sizes    
    df.lotsizesquarefeet.fillna(df.lotsizesquarefeet.median(), inplace = True)


    df.calculatedfinishedsquarefeet.fillna(df.calculatedfinishedsquarefeet.median(), inplace = True)

    # I am going to drop the rest of nulls 
    df.dropna(inplace = True)

    # Assuming your DataFrame is named 'df'
    for column in df.columns:
        df[column] = pd.to_numeric(df[column], errors='coerce')



    return df



################################### null rows ###################################

def nulls_by_row(df):
    num_missing = df.isnull().sum(axis=1)
    prcnt_miss = num_missing / df.shape[1] * 100
    rows_missing = pd.DataFrame({'num_cols_missing': num_missing, 'percent_cols_missing': prcnt_miss})\
    .reset_index()\
    .groupby(['num_cols_missing', 'percent_cols_missing']).count()\
    .rename(index=str, columns={'customer_id': 'num_rows'}).reset_index()
    return rows_missing




