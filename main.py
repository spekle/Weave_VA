from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel
from fastapi.responses import JSONResponse

app = FastAPI()
API_KEY = "Speklesolutions@123"

# Define a simple input model
class InputText(BaseModel):
    text: str
    
# Databricks notebook source
reference_date = '2024-09-30'
# Helper function to convert list to SQL string
def split_list_to_string(input_list):
    return "('" + "|".join([f"{x}" for x in input_list]) + "')"

# Helper function to convert list to SQL string
def split_list_to_brackets(input_list):
    return "(" + ",".join([f"'{x}'" for x in input_list]) + ")"


# --------------------- Configuration Dictionary ---------------------

# Updated master_dict with Separate Numerator and Denominator Filters
master_dict = {'groupings':{'anabolic':['brand_colname',['FORTEO','TYMLOS','EVENITY']],
                            'pcsk9':['brand_colname',['PRALUENT','REPATHA']],
                            'testgroup':['brand_colname',['OTEZLA','ENBREL','HUMIRA']]},
    'datamart_table_names': {
         'laad_weekly': {
            'inflamm': '`gco-application-dev`.bai_bcbu_user_adhoc.spk_enbrel_iqvia_datamart_weekly_final2'},
        'laad': {
            'inflamm': '`gco-application-dev`.bai_bcbu_user_adhoc.Spk_enbrel_iqvia_datamart_interim2',
            'resp': '`gco-application-dev`.bai_bcbu_user_adhoc.Spk_resp_iqvia_datamart3',
            'tavneos': '`gco-application-dev`.bai_bcbu_user_adhoc.Spk_iqvia_tavneos_datamart_final2',
            'bone': '`gco-application-dev`.bai_bcbu_user_adhoc.Spk_bone_iqvia_datamart4',
            'repatha':'`gco-application-dev`.bai_bcbu_user_adhoc.Spk_repatha_iqvia_datamart3'
        },
        'clinformatics':
            {'inflamm': '`gco-application-dev`.bai_bcbu_user_adhoc.enbrel_uhc_datamart_final2'},
        'copay':
            {'all': '`gco-application-dev`.bai_bcbu_user_adhoc.vw_s_patient_unalign_copay_claims3'},
        'emisar':
            {'inflamm': '`gco-application-dev`.bai_bcbu_user_adhoc.spk_emisar_upload_3'},
        'wac': {
            'all': '`gco-application-dev`.bai_bcbu_user_adhoc.spk_all_therapy_wac_asp'
        },
        'mmit': {
            'all': '`gco-application-dev`.bai_bcbu_user_adhoc.spk_otezla_um_pso_final'
        },
        'mmit_change': {
            'all': '`gco-application-dev`.bai_bcbu_user_adhoc.spk_otezla_um_pso_changes'
        },
        'npa': {
            'all': '`gco-application-dev`.bai_bcbu_user_adhoc.spk_npa_data'}
    },
    'other_constants': {
        'start_date': '2000-01-01',
        'metric_frequency': '',
        #'metric_type': ['trx'],
        'metrics': ['trx volume'],
        'end_date': '2030-01-01',
        'market_name': ['inflamm'],

        'product_name': {'inflamm': 'OTEZLA','bone':'PROLIA', 'tavneos':'TAVNEOS', 'wac': 'OTEZLA', 'resp': 'TEZSPIRE','repatha':'REPATHA'}
    },
    'universal_filters': {
        'laad': "claim_status in ('F', 'S') and flag_patientsupportprograms = 'Y'"},
    'metric_businessrules':
        {'laad': {
        'nrx approval rate': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('RV','PD') and  flag_rav_nrx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'nrx_approved_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV','RJ') AND flag_rav_nrx_colname = 'Y'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'Y',
            'denominator_alias': 'nrx_submitted_claims',
            'alias': 'nrx_approval_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }, 'resp': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('RV','PD') and flag_rav_nrx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'nrx_approved_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV','RJ') AND flag_rav_nrx_colname = 'Y'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'nrx_submitted_claims',
            'alias': 'nrx_approval_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx approval rate': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('RV','PD') and flag_rav_trx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'trx_approved_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV','RJ') AND flag_rav_trx_colname = 'Y'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'trx_submitted_claims',
            'alias': 'trx_approval_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nbrx approval rate': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('RV','PD') and lifecycle_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'nbrx_approved_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV','RJ') and lifecycle_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'nbrx_submitted_claims',
            'alias': 'nbrx_approval_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nrx abandonment rate': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('RV') and flag_rav_nrx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'nrx_abandoned_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV') AND flag_rav_nrx_colname = 'Y'",
            'denominator_aggregate': 'count',
            'denominator_alias': 'nrx_approved_claims',
            'denominator_distinct': 'y',
            'alias': 'nrx_abandonment_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx abandonment rate': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('RV') and flag_rav_trx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'trx_abandoned_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV') AND flag_rav_trx_colname = 'Y'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'trx_approved_claims',
            'alias': 'trx_abandonment_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nbrx abandonment rate': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('RV') and lifecycle_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'nbrx_abandoned_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV') and lifecycle_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'nbrx_approved_claims',
            'alias': 'nbrx_abandonment_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nrx fulfillment rate': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('PD') and flag_rav_nrx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'nrx_fulfilled_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV','RJ') AND flag_rav_nrx_colname = 'Y'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'nrx_submitted_claims',
            'alias': 'nrx_fulfillment_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx fulfillment rate': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('PD') and flag_rav_trx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'trx_fulfilled_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV','RJ') AND flag_rav_trx_colname = 'Y'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'trx_submitted_claims',
            'alias': 'trx_fulfillment_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nbrx fulfillment rate': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('PD') and lifecycle_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'nbrx_fulfilled_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV','RJ') and lifecycle_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'nbrx_submitted_claims',
            'alias': 'nbrx_fulfillment_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nbrx market share': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nbrx_product',
            'denominator_column': 'claim_colname',
            'denominator_filter': "flag_bof_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'nbrx_market',
            'alias': 'nbrx_market_share',
            'denominator_group_by_exclude': 'brand_colname',
            'additional_group_by': [],
            }},
        'nrx market share': {'all': {
            'ratio': 'Y',
            'numerator_column': 'nrx_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'nrx_product',
            'denominator_column': 'nrx_colname',
            'denominator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'denominator_aggregate': 'sum',
            'denominator_distinct': 'n',
            'denominator_alias': 'nrx_market',
            'alias': 'nrx_market_share',
            'denominator_group_by_exclude': 'brand_colname',
            'additional_group_by': [],
            }},
        'trx market share': {'all': {
            'ratio': 'Y',
            'numerator_column': 'trx_colname',
            'numerator_filter': "flag_bof_colname = 'Y'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'trx_product',
            'denominator_column': 'trx_colname',
            'denominator_filter': "flag_bof_colname = 'Y'",
            'denominator_aggregate': 'sum',
            'denominator_distinct': 'n',
            'denominator_alias': 'trx_market',
            'alias': 'trx_market_share',
            'denominator_group_by_exclude': 'brand_colname',
            'additional_group_by': [],
            }},
        'nbrx payer mix': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'nbrx_channel',
            'denominator_column': 'claim_colname',
            'denominator_filter': "flag_bof_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'denominator_aggregate': 'sum',
            'denominator_distinct': 'n',
            'denominator_alias': 'nbrx_total',
            'alias': 'nbrx_payer_mix',
            'denominator_group_by_exclude': 'channel_colname',
            'additional_group_by': ['channel_colname'],
            }},
        'nrx payer mix': {'all': {
            'ratio': 'Y',
            'numerator_column': 'nrx_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'nrx_channel',
            'denominator_column': 'nrx_colname',
            'denominator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'denominator_aggregate': 'sum',
            'denominator_distinct': 'n',
            'denominator_alias': 'nrx_total',
            'alias': 'nrx_payer_mix',
            'denominator_group_by_exclude': 'channel_colname',
            'additional_group_by': ['channel_colname'],
            }},
        'trx payer mix': {'all': {
            'ratio': 'Y',
            'numerator_column': 'trx_colname',
            'numerator_filter': "flag_bof_colname = 'Y'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'trx_channel',
            'denominator_column': 'trx_colname',
            'denominator_filter': "flag_bof_colname = 'Y'",
            'denominator_aggregate': 'sum',
            'denominator_distinct': 'n',
            'denominator_alias': 'trx_total',
            'alias': 'trx_payer_mix',
            'denominator_group_by_exclude': 'channel_colname',
            'additional_group_by': ['channel_colname'],
            }},
        'nbrx volume': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nbrx_volume',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nrx volume': {'all': {
            'ratio': 'n',
            'numerator_column': 'nrx_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'nrx_volume',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx volume': {'all': {
            'ratio': 'n',
            'numerator_column': 'trx_colname',
            'numerator_filter': "flag_bof_colname = 'Y'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'trx_volume',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }, 'resp': {
            'ratio': 'n',
            'numerator_column': 'trx_colname',
            'numerator_filter': "flag_bof_colname = 'Y'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'trx_volume',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nbrx claim volume': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nbrx_claim_volume',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nrx claim volume': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nrx_claim_volume',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx claim volume': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_bof_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'trx_claim_volume',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx submitted claims': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_rav_trx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'trx_submitted_claims',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx approved claims': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_rav_trx_colname = 'Y' and claim_type_colname in ('PD')",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'trx_approved_claims',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx rejected claims': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_rav_trx_colname = 'Y' and claim_type_colname in ('RJ')",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'trx_rejected_claims',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx reversed claims': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_rav_trx_colname = 'Y' and claim_type_colname in ('RV')",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'trx_reversed_claims',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nrx submitted claims': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_rav_nrx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nrx_submitted_claims',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nrx approved claims': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_rav_nrx_colname = 'Y' and claim_type_colname in ('PD')",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nrx_approved_claims',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nrx rejected claims': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_rav_nrx_colname = 'Y' and claim_type_colname in ('RJ')",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nrx_rejected_claims',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nrx reversed claims': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_rav_nrx_colname = 'Y' and claim_type_colname in ('RV')",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nrx_reversed_claims',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nbrx indication mix': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'nbrx_indication',
            'denominator_column': 'claim_colname',
            'denominator_filter': "flag_bof_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'denominator_aggregate': 'sum',
            'denominator_distinct': 'n',
            'denominator_alias': 'nbrx_total',
            'alias': 'nbrx_indication_mix',
            'denominator_group_by_exclude': 'indication_colname',
            'additional_group_by': ['indication_colname'],
            }},
        'nrx indication mix': {'all': {
            'ratio': 'Y',
            'numerator_column': 'nrx_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'nrx_indication',
            'denominator_column': 'nrx_colname',
            'denominator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'denominator_aggregate': 'sum',
            'denominator_distinct': 'n',
            'denominator_alias': 'nrx_total',
            'alias': 'nrx_indication_mix',
            'denominator_group_by_exclude': 'indication_colname',
            'additional_group_by': ['indication_colname'],
            }},
        'trx indication mix': {'all': {
            'ratio': 'Y',
            'numerator_column': 'trx_colname',
            'numerator_filter': "flag_bof_colname = 'Y'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'trx_indication',
            'denominator_column': 'trx_colname',
            'denominator_filter': "flag_bof_colname = 'Y'",
            'denominator_aggregate': 'sum',
            'denominator_distinct': 'n',
            'denominator_alias': 'trx_total',
            'alias': 'trx_indication_mix',
            'denominator_group_by_exclude': 'indication_colname',
            'additional_group_by': ['indication_colname'],
            }},
        'rejection reason': {'all': {
            'ratio': 'n',
            'numerator_column': 'claim_colname',
            'numerator_filter': "lifecycle_colname = 'Y' and claim_type_colname in ('RJ')  and newrefill_colname = 'New'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nrx_rejected_claims',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': ['reject_group_colname'],
            }},
        'post rejection': {'all': {
            'ratio': 'y',
            'numerator_column': 'patient_colname',
            'numerator_filter': "lifecycle_colname = 'Y' and claim_type_colname in ('RJ')  and newrefill_colname = 'New'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nrx_switch_patients',
            'denominator_column': 'patient_colname',
            'denominator_filter': "lifecycle_colname = 'Y' and claim_type_colname in ('RJ')  and newrefill_colname = 'New'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'nrx_rejected_patients',
            'alias': 'nrx_rejection_switch_rate',
            'denominator_group_by_exclude': ["next_brand_colname", "gap_to_next_fill_grp"],
            'additional_group_by': ["next_brand_colname", "gap_to_next_fill_grp"],
            }},
        'post abandonment': {'all': {
            'ratio': 'y',
            'numerator_column': 'patient_colname',
            'numerator_filter': "lifecycle_colname = 'Y' and claim_type_colname in ('RV')  and nbrx_sob_colname = 'NBRx'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'nrx_switch_patients',
            'denominator_column': 'patient_colname',
            'denominator_filter': "lifecycle_colname = 'Y' and claim_type_colname in ('RV')  and nbrx_sob_colname = 'NBRx'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'nrx_abandoned_patients',
            'alias': 'nrx_abandoned_switch_rate',
            'denominator_group_by_exclude': ["next_brand_colname", "gap_to_next_fill_grp"],
            'additional_group_by': ["next_brand_colname", "gap_to_next_fill_grp"],
            }},
        'oop cost': {'all': {
            'ratio': 'Y',
            'numerator_column': 'oop_colname',
            'numerator_filter': "flag_oop_colname = 'Y'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'total_oop',
            'denominator_column': 'claim_colname',
            'denominator_filter': "flag_oop_colname = 'Y'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'total_claims',
            'alias': 'avg_oop',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx life cycle mix': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('PD') and flag_rav_trx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'life_cycle_trx_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD') AND flag_bof_colname = 'Y'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'total_trx_claims',
            'alias': 'trx_life_cycle_mix',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'nrx life cycle mix': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('PD') and flag_rav_nrx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'life_cycle_nrx_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD') AND flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'total_nrx_claims',
            'alias': 'nrx_life_cycle_mix',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx life cycle mix': {'all': {
            'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and flag_rav_nbrx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'life_cycle_nbrx_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "flag_bof_colname = 'Y' and nbrx_sob_colname = 'NBRx'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'total_claims',
            'alias': 'life_cycle_mix',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'switch': {'all': {
            'ratio': 'N',
            'numerator_column': 'patient_colname',
            'numerator_filter': "claim_type_colname in ('PD')",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'patients',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': ['prev_brand_colname'],
            },'resp': {
            'ratio': 'N',
            'numerator_column': 'patient_colname',
            'numerator_filter': "claim_type_colname in ('PD')",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'patients',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': ['prev_brand_colname'],
            }},
            'patients': {'all': {
            'ratio': 'N',
            'numerator_column': 'patient_colname',
            'numerator_filter': "claim_type_colname in ('PD')",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'patients',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
             'willingness to pay': {'all': {
             'ratio': 'Y',
            'numerator_column': 'claim_colname',
            'numerator_filter': "claim_type_colname in ('RV') and flag_rav_nrx_colname = 'Y'",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'Y',
            'numerator_alias': 'nrx_abandoned_claims',
            'denominator_column': 'claim_colname',
            'denominator_filter': "claim_type_colname in ('PD','RV') AND flag_rav_nrx_colname = 'Y'",
            'denominator_aggregate': 'count',
            'denominator_distinct': 'y',
            'denominator_alias': 'nrx_approved_claims',
            'alias': 'nrx_abandonment_rate',
            'denominator_group_by_exclude': '',
            'additional_group_by': ['oop_group_colname'],
            }}
        
        },
        'clinformatics': {'nrx market share': {'all': {
            'ratio': 'Y',
            'numerator_column': 'nrx_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'nrx_product',
            'denominator_column': 'nrx_colname',
            'denominator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'denominator_aggregate': 'sum',
            'denominator_distinct': 'n',
            'denominator_alias': 'nrx_market',
            'alias': 'nrx_market_share',
            'denominator_group_by_exclude': 'brand_colname',
            'additional_group_by': [],
            }},
        'trx market share': {'all': {
            'ratio': 'Y',
            'numerator_column': 'trx_colname',
            'numerator_filter': "flag_bof_colname = 'Y'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'trx_product',
            'denominator_column': 'trx_colname',
            'denominator_filter': "flag_bof_colname = 'Y'",
            'denominator_aggregate': 'sum',
            'denominator_distinct': 'n',
            'denominator_alias': 'trx_market',
            'alias': 'trx_market_share',
            'denominator_group_by_exclude': 'brand_colname',
            'additional_group_by': [],
            }},
        'nrx volume': {'all': {
            'ratio': 'n',
            'numerator_column': 'nrx_colname',
            'numerator_filter': "flag_bof_colname = 'Y' and newrefill_colname = 'New'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'nrx_volume',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
        'trx volume': {'all': {
            'ratio': 'n',
            'numerator_column': 'trx_colname',
            'numerator_filter': "flag_bof_colname = 'Y'",
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'trx_volume',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }}},
        
        'wac': {'wac': {'all': {
        'ratio': 'n',
        'numerator_column': 'updated_wac',
        'numerator_filter': '',
        'numerator_aggregate': 'avg',
        'numerator_distinct': '',
        'numerator_alias': 'wac',
        'denominator_column': '',
        'denominator_filter': '',
        'denominator_aggregate': '',
        'denominator_distinct': '',
        'denominator_alias': '',
        'alias': '',
        'denominator_group_by_exclude': '',
        'additional_group_by': [],
        }}, 'asp': {'all': {
        'ratio': 'n',
        'numerator_column': 'updated_asp',
        'numerator_filter': '',
        'numerator_aggregate': 'avg',
        'numerator_distinct': '',
        'numerator_alias': 'asp',
        'denominator_column': '',
        'denominator_filter': '',
        'denominator_aggregate': '',
        'denominator_distinct': '',
        'denominator_alias': '',
        'alias': '',
        'denominator_group_by_exclude': '',
        'additional_group_by': [],
        }}},
        'mmit_change':
            {'coverage change': {'all': {
                'ratio': 'n',
                'numerator_column': 'lives_colname',
                'numerator_filter': "requirement_colname <> prev_requirement_colname and  prev_requirement_colname is not NULL and change_direction_colname <> 'Neutral'",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': 'coverage_change',
                'denominator_group_by_exclude': '',
                'additional_group_by': ['timeperiod_month_colname','change_type_colname','prev_requirement_colname','requirement_colname','change_direction_colname'],
                }},
             'specialist coverage change': {'all': {
                'ratio': 'n',
                'numerator_column': 'lives_colname',
                'numerator_filter': "requirement_colname <> prev_requirement_colname and  prev_requirement_colname is not NULL and change_direction_colname <>'Neutral' and change_type_colname ='Specialist'",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': 'specialist_coverage_change',
                'denominator_group_by_exclude': '',
                'additional_group_by': ['timeperiod_month_colname','change_type_colname','prev_requirement_colname','requirement_colname','change_direction_colname'],
                }},
             'bsa coverage change': {'all': {
                'ratio': 'n',
                'numerator_column': 'lives_colname',
                'numerator_filter': "requirement_colname <> prev_requirement_colname and  prev_requirement_colname is not NULL and change_direction_colname <> 'Neutral' and change_type_colname = 'BSA'",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': 'bsa_coverage_change',
                'denominator_group_by_exclude': '',
                'additional_group_by': ['timeperiod_month_colname','change_type_colname','prev_requirement_colname','requirement_colname','change_direction_colname'],
                }},
             'client status coverage change': {'all': {
                'ratio': 'n',
                'numerator_column': 'lives_colname',
                'numerator_filter': "requirement_colname <> prev_requirement_colname and  prev_requirement_colname is not NULL and change_direction_colname <> 'Neutral' and change_type_colname = 'Client Status'",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': 'client_status_coverage_change',
                'denominator_group_by_exclude': '',
                'additional_group_by': ['timeperiod_month_colname','change_type_colname','prev_requirement_colname','requirement_colname','change_direction_colname'],
                }},
             'status group coverage change': {'all': {
                'ratio': 'n',
                'numerator_column': 'lives_colname',
                'numerator_filter': "requirement_colname <> prev_requirement_colname and  prev_requirement_colname is not NULL and change_direction_colname <> 'Neutral' and change_type_colname = 'Status Group'",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': 'status_group_coverage_change',
                'denominator_group_by_exclude': '',
                'additional_group_by': ['timeperiod_month_colname','change_type_colname','prev_requirement_colname','requirement_colname','change_direction_colname'],
                }},
             'document coverage change': {'all': {
                'ratio': 'n',
                'numerator_column': 'lives_colname',
                'numerator_filter': "requirement_colname <> prev_requirement_colname and  prev_requirement_colname is not NULL and change_direction_colname <> 'Neutral' and change_type_colname = 'Document Change'",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': 'document_coverage_change',
                'denominator_group_by_exclude': '',
                'additional_group_by': ['timeperiod_month_colname','change_type_colname','prev_requirement_colname','requirement_colname','change_direction_colname'],
                }}},
         'npa':
            {'trx volume': {'all':
                {'ratio': 'n',
                'numerator_column': 'trx_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'trx_volume',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': '',
                'denominator_group_by_exclude': '',
                'additional_group_by': [],}},
             'trx market share': {'all':
                {'ratio': 'y',
                'numerator_column': 'trx_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'trx_product',
                'denominator_column': 'trx_colname',
                'denominator_filter': '',
                'denominator_aggregate': 'sum',
                'denominator_distinct': '',
                'denominator_alias': 'trx_market',
                'alias': 'trx_market_share',
                'denominator_group_by_exclude': 'brand_colname',
                'additional_group_by': [],}},
            'nrx volume': {'all':
                {'ratio': 'n',
                'numerator_column': 'nrx_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'nrx_volume',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': '',
                'denominator_group_by_exclude': '',
                'additional_group_by': [],}},
             'nrx market share': {'all':
                {'ratio': 'y',
                'numerator_column': 'nrx_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'nrx_product',
                'denominator_column': 'nrx_colname',
                'denominator_filter': '',
                'denominator_aggregate': 'sum',
                'denominator_distinct': '',
                'denominator_alias': 'nrx_market',
                'alias': 'nrx_market_share',
                'denominator_group_by_exclude': 'brand_colname',
                'additional_group_by': [],}},
            'trx dollars': {'all':
                {'ratio': 'n',
                'numerator_column': 'trxdollars_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'trx_dollars',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': '',
                'denominator_group_by_exclude': '',
                'additional_group_by': [],}},
             'trx dollars market share': {'all':
                {'ratio': 'y',
                'numerator_column': 'trxdollars_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'trxdollars_product',
                'denominator_column': 'trxdollars_colname',
                'denominator_filter': '',
                'denominator_aggregate': 'sum',
                'denominator_distinct': '',
                'denominator_alias': 'trxdollars_market',
                'alias': 'trxdollars__market_share',
                'denominator_group_by_exclude': 'brand_colname',
                'additional_group_by': [],}},
             'nrx dollars': {'all':
                {'ratio': 'n',
                'numerator_column': 'Nrxdollars_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'Nrx_dollars',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': '',
                'denominator_group_by_exclude': '',
                'additional_group_by': [],}},
             'nrx dollars market share': {'all':
                {'ratio': 'y',
                'numerator_column': 'nrxdollars_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'nrxdollars_product',
                'denominator_column': 'nrxdollars_colname',
                'denominator_filter': '',
                'denominator_aggregate': 'sum',
                'denominator_distinct': '',
                'denominator_alias': 'nrxdollars_market',
                'alias': 'nrxdollars__market_share',
                'denominator_group_by_exclude': 'brand_colname',
                'additional_group_by': [],}},
             
             },
             'emisar':
            {'trx volume': {'all': {
            'ratio': 'n',
            'numerator_column': 'trx_colname',
            'numerator_filter': '',
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'trx_volume',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }}},
            'copay':
            {'benefit amount': {'all': {
            'ratio': 'n',
            'numerator_column': 'benefit_amount_colname',
            'numerator_filter': '',
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'benefit_amount',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
             'copay amount': {'all': {
            'ratio': 'n',
            'numerator_column': 'copay_amount_colname',
            'numerator_filter': '',
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'copay_amount',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
             'oop amount': {'all': {
            'ratio': 'n',
            'numerator_column': 'oop_amount_colname',
            'numerator_filter': '',
            'numerator_aggregate': 'sum',
            'numerator_distinct': 'n',
            'numerator_alias': 'oop_amount',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }},
               'benefit recipients': {'all': {
            'ratio': 'n',
            'numerator_column': "patient_id_colname",
            'numerator_filter': "benefit_amount>0",
            'numerator_aggregate': 'count',
            'numerator_distinct': 'y',
            'numerator_alias': 'benefit_recipients',
            'denominator_column': '',
            'denominator_filter': '',
            'denominator_aggregate': '',
            'denominator_distinct': '',
            'denominator_alias': '',
            'alias': '',
            'denominator_group_by_exclude': '',
            'additional_group_by': [],
            }}},
        'mmit':
            {'coverage': {'all': {
                'ratio': 'y',
                'numerator_column': 'lives_colname',
                'numerator_filter': "status_colname = 'Covered'",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives_covered',
                'denominator_column': 'lives_colname',
                'denominator_filter': 'status_colname is not null',
                'denominator_aggregate': 'sum',
                'denominator_distinct': 'n',
                'denominator_alias': 'lives_total',
                'alias': 'pct_lives_covered',
                'denominator_group_by_exclude': '',
                'additional_group_by': [],
                }},
             'lives': {'all': {
                'ratio': 'n',
                'numerator_column': 'lives_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': 'n',
                'denominator_alias': '',
                'alias': '',
                'denominator_group_by_exclude': '',
                'additional_group_by': [],
                }},
             '1l coverage': {'all': {
                'ratio': 'y',
                'numerator_column': 'lives_colname',
                'numerator_filter': "status_colname = 'Covered' and steps_colname = 'No Step'",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives_1l',
                'denominator_column': 'lives_colname',
                'denominator_filter': 'status_colname is not null',
                'denominator_aggregate': 'sum',
                'denominator_distinct': 'n',
                'denominator_alias': 'lives_total',
                'alias': 'pct_1l_coverage',
                'denominator_group_by_exclude': '',
                'additional_group_by': [],
                }},
             'um criteria': {'all': {
                'ratio': 'n',
                'numerator_column': 'lives_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives_um',
                'denominator_column': 'lives_colname',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': 'lives_total',
                'alias': 'pct_lives_um',
                'denominator_group_by_exclude': '',
                'additional_group_by': ['status_colname', 'bsa_colname', 'spreq_colname', 'documentation_colname'],
                }}
             ,
             'step statement': {'all': {
                'ratio': 'n',
                'numerator_column': 'lives_colname',
                'numerator_filter': "",
                'numerator_aggregate': 'sum',
                'numerator_distinct': 'n',
                'numerator_alias': 'lives',
                'denominator_column': '',
                'denominator_filter': '',
                'denominator_aggregate': '',
                'denominator_distinct': '',
                'denominator_alias': '',
                'alias': '',
                'denominator_group_by_exclude': '',
                'additional_group_by': ['step_statement_colname'],
                }}
             }},
            
    'sql_colnames': {

        'mmit':{'all':{
                "brand_colname": "drug",
                "controller_colname": "controller",
                "channel_colname": "channel_grouped",
                "lives_colname": "lives",
                "status_colname": "status_group",
                "indication_colname": "indication_name",
                "timeperiod_month_colname": "period",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank',
                "clientstatus_colname": "client_status",
                "spreq_colname": "specialist_requirement",
                "steps_colname": "client_status",
                "bsa_colname": "bsa_requirement",
                "documentation_colname": "document_diagnosis_requirement",
                "step_statement_colname": "step_statement"
                }},


        'laad': {
                'inflamm': {
                "brand_colname": "brand_name",
                "controller_colname": "mmit_controller_name",
                "channel_colname": "method_of_payment_final",
                "pbm_colname": "pbm_name",
                "rmo_colname": "rmo",
                "indication_colname": "indication_max",
                "timeperiod_month_colname": "month_year",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                "fill_date_colname": "fill_date",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                "reject_reason_colname":"reject_reason",
                "reject_group_colname":"reject_group",
                "trx_colname": "rx",
                "nrx_colname": "rx",
                "nbrx_colname": "claim_id",
                "claim_colname": "claim_id",
                "patient_colname": "patient_id",
                "claim_type_colname":"claim_type",
                "claim_status_colname":"claim_status",
                "flag_bof_colname": "flag_bof",
                "lifecycle_colname":"life_cycle_yn",
                "plan_name_colname":"plan_name",
                "flag_rav_nrx_colname": "flag_rav_nrx",
                "specialty_colname": "physician_specialty",
                "flag_rav_trx_colname": "flag_rav_trx",
                "nbrx_sob_colname": "nbrx_sob",
                'oop_colname': "patient_opc",
                'flag_oop_colname': "flag_oop",
                'newrefill_colname': 'new_rx_flag',
                'gap_to_next_fill_colname': 'gap_to_next_fill',
                'next_brand_colname': 'next_claim_brand_name',
                'prev_brand_colname': 'prev_claim_brand_name',
                'month_rank_colname': 'month_rank',
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank',
                'oop_group_colname': 'patient_opc_group',
                'medicare_patienttype_colname': 'eligibility_class'
                # 'oop_colname': 'patient_opc_group'
                # Add other column mappings as needed
            },
            'bone': {
                "brand_colname": "brand",
                "controller_colname": "mmit_controller_name",
                "channel_colname": "method_of_payment_renamed_new",
                "pbm_colname": "pbm",
                "rmo_colname": "rmo",
                "indication_colname": "indication_max",
                "timeperiod_month_colname": "month_year",
                "fill_date_colname": "service_or_fill_date",
                "specialty_colname": "physician_specialty",
                "timeperiod_year_colname": "year",
                "reject_reason_colname":"reject_reason",
                "timeperiod_quarter_colname": "quarter",
                "trx_colname": "rx",
                "nrx_colname": "rx",
                "nbrx_colname": "claim_id",
                "plan_name_colname":"plan_name",
                "claim_colname": "claim_id",
                "claim_type_colname":"claim_type",
                "claimstatus_colname":"claim_status",
                "flag_bof_colname": "flag_bof",
                "claim_status_colname": "claim_status",
                "lifecycle_colname":"life_cycle_yn" ,
                "reject_group_colname":"reject_group",
                'month_rank_colname': 'month_rank',
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank',
                'newrefill_colname': 'new_rx_flag'

                # Add other column mappings as needed
            
        },
        'tavneos': {
                "brand_colname": "product_name",
                "controller_colname": "mmit_controller_name",
                "channel_colname": "channel_final",
                "pbm_colname": "pbm",
                "rmo_colname": "rmo",
                "indication_colname": "indication_max",
                "specialty_colname": "specialty_descriptio",
                "timeperiod_month_colname": "month_year",
                "fill_date_colname": "fill_date",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                "reject_reason_colname":"reject_reason",
                "plan_name_colname":"plan_name",
                "trx_colname": "rx",
                "nrx_colname": "rx",
                "nbrx_colname": "claim_id",
                "claim_colname": "claim_id",
                "claim_type_colname":"claim_type",
                "claimstatus_colname":"claim_status",
                "flag_bof_colname": "flag_bof",
                "claim_status_colname": "claim_status",
                "lifecycle_colname":"life_cycle_yn" ,
                'month_rank_colname': 'month_rank',
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank',
                'newrefill_colname': 'new_rx_flag'

                # Add other column mappings as needed
        },
        'resp': {
                "brand_colname": "brand_name",
                "controller_colname": "mmit_controller_name",
                "channel_colname": "method_of_payment",
                "pbm_colname": "pbm_name",
                "rmo_colname": "rmo",
                "indication_colname": "diagnosis_type_final",
                "specialty_colname": "specialty_description",
                "timeperiod_month_colname": "month_year",
                "year_colname": "year",
                "month_colname": "month_year",
                "quarter_colname": "quarter",
                "fill_date_colname": "service_date",
                "plan_name_colname":"plan_name",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                "reject_reason_colname":"reject_reason",
                "trx_colname": "projected_claim_key",
                "nrx_colname": "projected_claim_key",
                "nbrx_colname": "projected_claim_key_nbrx",
                "claim_colname": "claim_key",
                "claim_type_colname":"claim_type",
                "claimstatus_colname":"claim_status",
                "flag_bof_colname": "flag_bof",
                "claim_status_colname": "claim_status",
                "lifecycle_colname":"life_cycle_yn",
                "benefittype_colname":"benefit_type",
                "flag_rav_nrx_colname": "flag_rav_nrx",
                "flag_rav_trx_colname": "flag_rav_trx",
                "nbrx_sob_colname": "nbrx_sob",
                'month_rank_colname': 'month_rank',
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank',
                'newrefill_colname': 'new_rx_flag'
                # Add other column mappings as needed
            
        },'repatha': {
                "brand_colname": "product",
                "controller_colname": "mmit_controller_name",
                "channel_colname": "method_of_payment_final",
                "pbm_colname": "pbm_name",
                "rmo_colname": "rmo",
                "indication_colname": "indication_max",
                "specialty_colname": "physician_specialty",
                "timeperiod_month_colname": "month_year",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                "fill_date_colname": "fill_date",
                "reject_reason_colname":"reject_reason",
                "plan_name_colname":"plan_name",
                "trx_colname": "rx",
                "nrx_colname": "rx",
                "nbrx_colname": "rx",
                "claim_colname": "claim_id",
                "patient_colname": "patient_id",
                "claim_type_colname":"claim_type",
                "claim_status_colname":"claim_status",
                "flag_bof_colname": "flag_bof",
                "lifecycle_colname":"life_cycle_yn",
                "flag_rav_nrx_colname": "flag_rav_nrx",
                "flag_rav_trx_colname": "flag_rav_trx",
                "nbrx_sob_colname": "nbrx_sob",
                'month_rank_colname': 'month_rank',
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank',
                'oop_colname': "patient_opc",
                'flag_oop_colname': "flag_oop"
                # Add other column mappings as needed
            }},
        
        'laad_weekly': {'inflamm': {
                "brand_colname": "brand_name",
                "controller_colname": "mmit_controller_name",
                "channel_colname": "method_of_payment_final",
                "pbm_colname": "pbm_name",
                "rmo_colname": "rmo",
                "indication_colname": "indication_max",
                "timeperiod_month_colname": "month_year",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                "fill_date_colname": "fill_date",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                "reject_reason_colname":"reject_reason",
                "reject_group_colname":"reject_group",
                "trx_colname": "rx",
                "nrx_colname": "rx",
                "nbrx_colname": "claim_id",
                "claim_colname": "claim_id",
                "patient_colname": "patient_id",
                "claim_type_colname":"claim_type",
                "claim_status_colname":"claim_status",
                "flag_bof_colname": "flag_bof",
                "lifecycle_colname":"life_cycle_yn",
                "plan_name_colname":"plan_name",
                "flag_rav_nrx_colname": "flag_rav_nrx",
                "specialty_colname": "physician_specialty",
                "flag_rav_trx_colname": "flag_rav_trx",
                "nbrx_sob_colname": "nbrx_sob",
                'oop_colname': "patient_opc",
                'flag_oop_colname': "flag_oop",
                'newrefill_colname': 'new_rx_flag',
                'gap_to_next_fill_colname': 'gap_to_next_fill',
                'next_brand_colname': 'next_claim_brand_name',
                'prev_brand_colname': 'prev_claim_brand_name',
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank',
                'oop_group_colname': 'patient_opc_group',
                'medicare_patienttype_colname': 'eligibility_class'
                # 'oop_colname': 'patient_opc_group'
                # Add other column mappings as needed
            }},
        'wac': {'all': {
                "brand_colname": "brand_name",
                "wac_colname": "updated_wac",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                "timeperiod_month_colname": "month_year",
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank'
            }},
        'mmit_change': {'all': {
                "brand_colname": "drug",
                "channel_colname": "channel",
                "indication_colname": "indication_name",
                "requirement_colname": "requirement",
                "prev_requirement_colname": "prev_requirement",
                "next_requirement_colname": "next_requirement",
                "change_colname": "requirement_change",
                "change_type_colname": "change_type",
                "lives_colname": "lives",
                "prev_lives_colname": "prev_lives",
                "next_lives_colname": "next_lives",
                "change_direction_colname": "requirement_change_direction",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                "timeperiod_month_colname": "month_year",
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank'
            }},
        'clinformatics': {'all': {
                "brand_colname": "product",
                "channel_colname": "channel",
                "indication_colname": "indication",
                "specialty_colname": "primary_specialty_dscr_1",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "qtr",
                "timeperiod_month_colname": "month_year",
                "trx_colname": "trx",
                "nrx_colname": "trx",
                "newrefill_colname": "new_refill",
                "flag_bof_colname": "flag_bof",
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank'
            }},

        'copay': {'all': {
                "brand_colname": "brand_normalized_name",
                "channel_colname": "channel",
                "claim_source_colname":"claim_source",
                "benefit_amount_colname":"benefit_amount",
                "normalized_benefit_amount_colname":"normalized_benefit_amount",
                "oop_amount_colname":"oop_amount",
                "copay_amount_colname":"copay_amount",
                "days_supply_colname":"days_of_supply",
                "pharmacy_name_colname":"pharmacy_name",
                "sp_group_colname":"sp_group",
                "patient_id_colname":"patient_id",
                "specialty_colname": "hcp_specialty_1",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "quarter",
                "timeperiod_month_colname": "year_month",
                "flag_bof_colname": "flag_bof",
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank'
            }},
        'emisar': {'all': {
                "brand_colname": "drug_name",
                "wac_spend_colname": "wac_spend",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "qtr",
                "timeperiod_month_colname": "month_year",
                "trx_colname": "total_number_of_prescriptions",
                "nrx_colname": "total_number_of_prescriptions",
                # "newrefill_colname": "new_refill",
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank',
                'days_supply_colname': 'days_supply',
                'wac_spend_colname':'wac_spend',
                'total_quantity_colname': 'total_quantity'
            }},
        
        'npa': {'all': {
                "brand_colname": "brand_normalized_name",
                "market_colname": "market_name",
                "timeperiod_year_colname": "year",
                "timeperiod_quarter_colname": "year_quarter",
                "timeperiod_month_colname": "year_month",
                'year_rank_colname': 'year_rank',
                'quarter_rank_colname': 'quarter_rank',
                'month_rank_colname': 'month_rank',
                'month_yearrank_colname': 'month_yearrank',
                'month_quarterrank_colname': 'month_quarterrank',

                "trx_colname": "trx",
                "nrx_colname": "nrx",
                "trxdollars_colname": "trx_dollars",
                "nrxdollars_colname": "nrx_dollars",
                
            }}
        }}
                # Additional markets can be added here

metrics_business_rules_text_dict = {'laad': {'all':
      {'trx market share': 'Includes all fulfilled claims. Market share is based on rx which is (days of supply)/30',
               'nrx market share': 'Includes new fulfilled claims. Market share is based on rx which is (days of supply)/30',
               'nbrx market share': 'Includes new to brand fulfilled claims. Market share is based on rx which is (days of supply)/30',
               'trx volume': 'Includes all fulfilled claims. Volume is based on rx which is (days of supply)/30',
               'trx submitted claims': 'Includes only life cycle claims. Includes PD, RV and RJ claims. Volume is based on trx which is (days of supply)/30',
               'trx approved claims': 'Includes only life cycle claims. Includes only PD claims. Volume is based on rx which is (days of supply)/30',
               'trx rejected claims': 'Includes only life cycle claims. Includes only RJ claims. Volume is based on rx which is (days of supply)/30',
               'trx reversed claims': 'Includes only life cycle claims. Includes only RV claims. Volume is based on rx which is (days of supply)/30',
               'nrx submitted claims': 'Includes only new life cycle claims. Includes PD, RV and RJ claims. Volume is based on nrx which is (days of supply)/30',
               'nrx approved claims': 'Includes only new life cycle claims. Includes only PD claims. Volume is based on nrx which is (days of supply)/30',
               'nrx rejected claims': 'Includes only new life cycle claims. Includes only RJ claims. Volume is based on nrx which is (days of supply)/30',
               'nrx reversed claims': 'Includes only new life cycle claims. Includes only RV claims. Volume is based on nrx which is (days of supply)/30',
               'nrx volume': 'Includes new fulfilled claims. Volume is based on nrx which is (days of supply)/30',
               'nbrx volume': 'Includes nbrx fulfilled claims. Volume is based on nbrx claim count',
               'trx claim volume': 'Includes all fulfilled claims. Volume is based on trx claim count',
               'nrx claim volume': 'Includes new fulfilled claims. Volume is based on nrx claim count',
               'nbrx claim volume': 'Includes nbrx fulfilled claims. Volume is based on nbrx claim count',
               'nrx approval rate': 'Includes only new life cycle claims. Approval rate is defined as (PD + RV) / (PD + RV + RJ) claims',
               'nrx abandonment rate': 'Includes only new life cycle claims. Abandonment rate is defined as (RV) / (PD + RV) claims',
               'nrx fulfillment rate': 'Includes only new life cycle claims. Fulfillment rate is defined as (PD) / (PD + RV + RJ) claims',
               'trx approval rate': 'Includes only life cycle claims. Approval rate is defined as (PD + RV) / (PD + RV + RJ) claims',
               'trx abandonment rate': 'Includes only life cycle claims. Abandonment rate is defined as (RV) / (PD + RV) claims',
               'trx fulfillment rate': 'Includes only life cycle claims. Fulfillment rate is defined as (PD) / (PD + RV + RJ) claims',
               'nbrx approval rate': 'Includes only new to brand life cycle claims. Approval rate is defined as (PD + RV) / (PD + RV + RJ) claims',
               'nbrx abandonment rate': 'Includes only new to brand life cycle claims. Abandonment rate is defined as (RV) / (PD + RV) claims',
               'nbrx fulfillment rate': 'Includes only new to brand life cycle claims. Fulfillment rate is defined as (PD) / (PD + RV + RJ) claims',
               'oop cost': 'Includes only claims with valid patient oop of pocket. Only claims with days of supply between 28-30 are considered',
               'trx payer mix': 'Includes all fulfilled claims. Volume is based on trx which is (days of supply)/30. Commercial channel includes Assistance',
               'nrx payer mix': 'Includes new fulfilled claims. Volume is based on nrx which is (days of supply)/30. Commercial channel includes Assistance',
               'nbrx payer mix': 'Includes new to brand fulfilled claims. Volume is based on nbrx claim count. Commercial channel includes Assistance',
               'nbrx indication mix': 'Includes new to brand fulfilled claims. Volume is based on nbrx claim count',
               'nrx indication mix': 'Includes new fulfilled claims. Volume is based on nrx which is (days of supply)/30',
               'trx indication mix': 'Includes all fulfilled claims. Volume is based on trx which is (days of supply)/30',
               'patients': 'Total unique patients',
               'nbrx patients': 'New to brand patients',
               'bio-naive': ['Bio naive', 'Bio-naive'],
               'switch': 'Unique patients switching from one brand to another brand',
               'rejection reason': 'Includes only new life cycle claims. Includes only RJ claims',
               'post rejection': 'Includes all fulfilled claims post rejected claim',
               'post abandonment': 'Includes all fulfilled claims post abandoned claim',
               'lis contribution': "Includes only fulfilled claims in Medicare channel. STD includes 'STD' and 'Not Part D' and LIS includes 'LID-DE', 'LIS-DTC', 'LIS-NON-DE' and 'LIS-UNKNOWN'",
               'willingness to pay': "Includes only new life cycle claims with valid patient oop cost. Abandonment rate is defined as (RV) / (PD + RV) claims. Only claims with days of supply between 28-30 are considered",
               'trx life cycle mix': "Includes all fulfilled claims. Mix is based on trx which is (days of supply)/30",
               'nrx life cycle mix': "Includes new fulfilled claims. Mix is based on trx which is (days of supply)/30",
               'nbrx life cycle mix': "Includes new to brand fulfilled claims. Mix is based on trx which is (days of supply)/30",
               
               'wac': 'WAC price for 30 day supply of the drug',
               'asp': 'Average seeling price for 30 day supply of the drug',
               'clinformatics trx': ['trx from clinformatics','clinformatics trx volume','clinformatics trx'],
               'clinformatics nrx': ['nrx from clinformatics','clinformatics nrx volume','clinformatics nrx'],
               'coverage change': ['coverage change', 'coverage change rate', 'coverage change','change in coverage'],
               'trx dollars': ['trx dollars', 'trx dollar volume', 'volume in trx dollars'],
               'nrx dollars': ['nrx dollars', 'nrx dollar volume', 'volume in nrx dollars'],
               'trx dollars market share': ['trx dollars market share', 'trx dollar share', 'share in trx dollars'],
               'nrx dollars market share': ['nrx dollars market share', 'nrx dollar share', 'share in nrx dollars']
               }           },
            'mmit': {'all' : {'lives': 'Includes all lives',
               'coverage': "Covered lives include 'Covered', 'Covered (PA/ST)', 'Preferred', and 'Preferred (PA/ST)",
               'um criteria': "Includes all UM criteria - coverage, step requirement, specialist requirement and bsa requirements",
               '1l coverage': "First line coverage is defined as lives that have access to brand without going through a step"
               }},
            'npa': {'all' :
                  {'trx dollars': 'Total utilization in dollars',
               'nrx dollars': 'New utilization in dollars',
               'trx dollars market share': 'Market share based on dollar utilization',
               'nrx dollars market share': 'Market share based on new dollar utilization'}},
            'copay': {'all' :
                  {'benefit amount': 'Total benefit amount in dollars',
               'copay amount': 'Total copay amount in dollars',
               'oop amount': 'Total out-of-pocket in dollars',
               'benefit recipients': 'Total unique patients with more than $0 benefit amount'}}}
# COMMAND ----------



entity_fuzzymatch_threshold_lst = {'metrics': 80, 'channel_names': 80, 'brand_names':80, 'controller_names': 80, 'pbm_names': 80, 'rmo_names': 80, 'indication_names': 80, 'rmo_names': 80, 'benefittype_names': 90, 'formulary_names': 80}
entity_fuzzymatch_threshold = 80


market_products_dict = {
    'inflamm': ['enbrel', 'humira', 'otezla', 'siliq', 'simponi', 'rinvoq', 'skyrizi', 'olumiant', 'cosentyx', 'xeljanz', 'ilumya', 'kevzara', 'kineret', 'stelara', 'taltz', 'actemra', 'cimzia', 'orencia', 'tremfya'],
    'bone': ['prolia', 'evenity', 'za', 'actonel', 'alendronate', 'atelvia', 'binosto', 'boniva', 'evenity', 'evista', 'forteo', 'fosamax', 'ibandronate', 'raloxifene', 'reclast', 'risedronate', 'teriparatide', 'tymlos', 'xgeva'],
    'tavneos': ['tavneos'],
    'resp': ['tezspire', 'nucala', 'fasenra', 'xolair', 'cinqair', 'dupixent'],
    'repatha': ['repatha', 'praluent', 'nexlitol', 'nexlizet', 'leqvio']
}

datasource_dict={'laad': ['LAAD'],
                 'laad_weekly': ['LAAD weekly','laad week','weekly laad','laad week','mjqw'],
                 'wac': ['WAC','pricing'],
                 'mmit': ['MMIT','coverage'],
                 'mmit_change': ['coverage change','change in coverage'], 'clinformatics':['clinformatics', 'cmm'],'emisar':['emisar data source', 'emisaar data source'], 'npa': ['npa','national prescriber audit'], 'copay': ['copay','co-pay']}


datasource_metrics_dict = {
        'laad': ['trx market share','nrx market share', 'nbrx market share', 'trx volume', 'nrx volume','nbrx volume',
               'trx claim volume','nrx claim volume','nbrx claim volume','nrx approval rate','trx submitted claims','trx approved claims','trx rejected claims','trx reversed claims','nrx submitted claims','nrx approved claims','nrx rejected claims','nrx reversed claims',
               'nrx abandonment rate','nrx fulfilment rate' ,'trx approval rate','trx abandonment rate','trx fulfilment rate', 'nbrx approval rate','nbrx abandonment rate','nbrx fulfilment rate', 'rejection reason', 'post rejection', 'post abandonment',
               'oop cost','payer mix','trx indication mix', 'nrx indication mix', 'nbrx indication mix','patients', 'nbrx patients','bio-naive', 'switch','trx payer mix','lis contribution', 'willingness to pay', 'nrx life cycle mix', 'trx life cycle mix', 'nbrx life cycle mix'],
     'wac': ['wac', 'asp'],
     'mmit': ['coverage', 'um criteria', '1l coverage', 'step statement', 'lives'],
     'mmit_change': ['coverage change', 'specialist coverage change','client status coverage change','bsa coverage change','status group coverage change','document coverage change'],
     'emisar': ['prescriptions'],
     'copay': ['benefit amount','copay amount','oop amount','benefit recipients']}

metrictype_dict = {'trx': ['TRx', 'total scripts'],
               'nrx': ['nrx', 'new scripts'],
               'nbrx': ['nbrx', 'new to brand', 'new starts']}
               
metricname_dict = {
        'volume': ['volume', 'demand', 'scripts', 'utilization', 'business'],
         'market_share': ['share', 'market share'],
         'approval_rate':['approval rate','approval','app rate','approved'],
         'rejection_rate':['rejection rate','rejection','rej rate','rejected'],
         'abandonment_rate':['abandonment rate','abandonment','aban rate','abandoned'],
         'fulfilment_rate':['fulfilment rate','fulfilment','fullfilment rate','fulfillment','fullfilment','fulfilled'],
         'payer_mix':['payer mix', 'channel contribution', 'channel mix'],
        'oop': ['patient oop', 'oop', 'oop cost','patient out of pocket cost'],
        'indication_mix': ['indication mix', 'indication contribution']

        }
commonwords_dict = ['rate', 'share', 'claims', 'mix', 'patients', 'requirement', 'req', 'contribution']


metrics_dict = {'trx market share': ['market share', 'share', 'TRx share', 'TRx market share'],
               'nrx market share': ['nrx market share', 'nrx share'],
               'nbrx market share': ['nbrx market share', 'nbrx share'],
               'trx volume': ['TRx volume', 'total scripts', 'utilization', 'business','volume'],
               'trx submitted claims': ['trx demand', 'total demand', 'top funnel volume', 'submitted claims','submitted','trx submitted claims','trx submitted', 'trx top funnel volume'],
               'trx approved claims': ['approved claims','trx approved claims'],
               'trx rejected claims': ['rejected claims','trx rejected claims'],
               'trx reversed claims': ['reversed claims','trx reversed claims'],
               'nrx submitted claims': ['nrx top funnel volume', 'new top funnel volume', 'nrx submitted claims','nrx submitted'],
               'nrx approved claims': ['nrx approved claims',],
               'nrx rejected claims': ['nrx rejected claims'],
               'nrx reversed claims': ['nrx reversed claims'],
               'nrx volume': ['NRx volume',  'new scripts'],
               'nbrx volume': ['NBRx volume'],
               'trx claim volume': ['TRx claim volume', 'trx claim count', 'trx claims'],
               'nrx claim volume': ['NRx claim volume', 'nrx claim count', 'nrx claims'],
               'nbrx claim volume': ['NBRx claim volume', 'nbrx claim count', 'nbrx claims'],
               'nrx approval rate': ['nrx approval', 'approval rate', 'nrx approval rate', 'new approval rate', 'approval rate for new claims', 'rejection rate', 'nrx rejection rate','rejection rate for new claims'],
               'nrx abandonment rate': ['nrx abandonment', 'abandonment rate', 'nrx abandonment rate', 'new abandonment rate'],
               'nrx fulfillment rate': ['nrx fulfillment', 'fulfillment rate', 'nrx fulfillment rate', 'new fulfillment rate'],
               'trx approval rate': ['trx approval', 'trx approval rate', 'total approval rate'],
               'trx abandonment rate': ['trx abandonment', 'trx abandonment rate', 'total abandonment rate'],
               'trx fulfillment rate': ['trx fulfillment', 'trx fulfillment rate', 'total fulfillment rate'],
               'nbrx approval rate': ['nbrx approval', 'nbrx approval rate', 'new to brand approval rate', 'approval rate for new patients', 'nbrx rejection rate'],
               'nbrx abandonment rate': ['nbrx abandonment', 'nbrx abandonment rate', 'new to brand abandonment rate'],
               'nbrx fulfillment rate': ['nbrx fulfillment', 'nbrx fulfillment rate', 'new to brand fulfillment rate'],
               'oop cost': ['patient oop', 'oop cost','patient out of pocket cost','avg oop'],
               'trx payer mix': ['payer mix', 'channel contribution', 'channel mix'],
               'nrx payer mix': ['new claims payer mix', 'new claims channel contribution', 'new claims channel mix', 'new rx payer mix', 'new rx channel contribution', 'new rx channel mix'],
               'nbrx payer mix': ['nbrx payer mix', 'new to brand payer mix', 'nbrx channel contribution', 'nbrx channel contribution', 'nbrx channel mix'],
               'nbrx indication mix': ['nbrx indication mix', 'nbrx indication contribution'],
               'nrx indication mix': ['nrx indication mix', 'nrx indication contribution'],
               'trx indication mix': ['trx indication mix', 'trx indication contribution', 'indication mix', 'indication contribution'],
               'patients': ['total patient', 'count patients', 'many patients','patient count'],
               'nbrx patients': ['nbrx patient', 'nbrx patients', 'new brand patients'],
               'bio-naive': ['Bio naive', 'Bio-naive'],
               'switch': ['switch', 'switching', 'patient switch', 'patients switching', 'patient switching'],
               'rejection reason':['Rejection reasons', 'rejection type'],
               'post rejection': ['after rejection', 'post rejection', 'rejected patients switch', 'post rejection utilization', 'after rejection', 'after getting rejected', 'after rejected', 'after being rejected', 'post getting rejected'],
               'post abandonment': ['after abandonment', 'post abandonment', 'abandoned patients switch', 'post abandonment utilization', 'after reversing', 'post reversing', 'reveresed patients switch', 'post reversed utilization', 'after reversing', 'after abandoned', 'after getting abandoned', 'after abandoning'],
               'lis contribution': ['LIS patient contribution', 'LIS contribution', '% LIS contribution'],
               'willingness to pay': ['willingness pay', 'willingness pay curve', 'ability pay', 'ability pay curve','abandonment increase with patient OOP increase'],
               'trx life cycle mix': ['life cycle mix', 'proportion of life cycle claims', 'proportion of life cycle','life cycle vs. non life cycle','life vs. nonlife', 'life cycle contribution', 'life cycle contribute'],
               'nrx life cycle mix': ['nrx life cycle mix', 'proportion of nrx life cycle claims', 'proportion of nrx life cycle','nrx life cycle vs. non life cycle','nrx life vs. nonlife', 'nrx life cycle contribution', 'nrx life cycle contribute'],
               'nbrx life cycle mix': ['nbrx life cycle mix', 'proportion of nbrx life cycle claims', 'proportion of nbrx life cycle','nbrx life cycle vs. non life cycle','nbrx life vs. nonlife', 'nbrx life cycle contribution', 'nbrx life cycle contribute'],
               'lives': ['lives'],
               'coverage': ['Coverage', 'mmit coverage', 'covered', 'not covered', 'preferred', 'covered lives', 'preferred lives', 'not covered lives', '% covered lives', '% covered', '% not covered'],
               'um criteria': ['um criteria', 'um details', 'utilization management criteria', 'utilization management needs'],
               '1l coverage': ['first line coverage', '1l coverage', '1st line coverage'],
               'wac': ['wac', 'wholesale acquisition cost', 'price', 'list price'],
               'asp': ['asp', 'average sales price', 'average selling price', 'avg sales price', 'avg selling price'],
               'clinformatics trx': ['trx from clinformatics','clinformatics trx volume','clinformatics trx'],
               'clinformatics nrx': ['nrx from clinformatics','clinformatics nrx volume','clinformatics nrx'],
               'coverage change': ['coverage change', 'coverage change rate', 'coverage change','change in coverage'],
               'specialist status coverage change': ['spec coverage change', 'specialist coverage change', 'specialty coverage change','change in specialist coverage','specialist coverage change','change in specialist coverage'],
               'bsa coverage change': ['bsa coverage change', 'bsa coverage change rate', 'bsa coverage change','change in bsa coverage','change in bsa','bsa change'],
               'client status coverage change': ['client status coverage change', 'client coverage change', 'client status coverage change','change in client status coverage','client status change','change in client status'],
               'status group coverage change': ['status group coverage change', 'status group change','change in status group coverage','status group change','change in status group'],
               'document coverage change': ['document coverage change', 'documentation coverage change', 'documentation change','change in documentation coverage','change in documentation'],
               'trx dollars': ['trx dollars', 'trx dollar volume', 'volume in trx dollars'],
               'nrx dollars': ['nrx dollars', 'nrx dollar volume', 'volume in nrx dollars'],
               'trx dollars market share': ['trx dollars market share', 'trx dollar share', 'share in trx dollars'],
               'nrx dollars market share': ['nrx dollars market share', 'nrx dollar share', 'share in nrx dollars'],
               'benefit amount':['benefit amount','total benefit','benefit paid'],
               'copay amount':['copay amount','copay paid'],
               'oop amount':['oop amount','out of pocket amount','oop paid'],
               'benefit recipients':['benefit recipients','total benefit recipients']}

brand_dict = {'enbrel': ['Enbrel'],
             'humira': ['Humira'],
             'otezla': ['Otezla'],
             'siliq': ['Siliq'],
             'simponi': ['Simponi'],
             'rinvoq': ['Rinvoq'],
             'skyrizi': ['Skyrizi'],
             'olumiant': ['Olumiant'],
             'cosentyx': ['Cosentyx'],
             'xeljanz': ['Xeljanz'],
             'ilumya': ['Ilumya'],
             'kevzara': ['Kevzara'],
             'kineret': ['Kineret'],
             'stelara': ['Stelara'],
             'taltz': ['Taltz'],
             'actemra': ['Actemra'],
             'cimzia': ['Cimzia'],
             'orencia': ['Orencia'],
             'tremfya': ['Tremfya']   ,
             'prolia': ['Prolia']   ,
             'evenity': ['Evenity']   ,
             'za': ['ZA', 'Zoledronic Acid'],
             'actonel': ['actonel', 'actonel w/calc', 'actonel with calcium'],
             'alendronate': ['alendronate', 'alendronate sodium', 'alendronate sod'],
             'atelvia': ['atelvia'],
             'binosto': ['binosto'],
             'bonivo': ['bonivo'],
             'evista': ['evista'],
             'forteo': ['forteo'],
             'fosamax': ['fosamax', 'fosamax plus d'],
             'ibandronate': ['ibandronate', 'ibandronate sodium', 'ibandronate sod'],
             'raloxifene': ['raloxifene','raloxifene hcl'],
             'reclast': ['reclast'],
             'risedronate': ['risedronate', 'risedronate sod', 'risedronate sod dr'],
             'teriparatide': ['teriparatide'],
             'tymlos': ['tymlos'],
             'xgeva': ['xgeva'],
             'tavneos': ['Tavneos']   ,
             'tezspire': ['Tezspire', 'Teszpire PFP', 'Tezspire PFS']   ,
             'nucala': ['Nucala', 'Nucala AI', 'Nucala PFS'],
             'fasenra': ['Fasenra', 'Fasenra AI'],
             'xolair': ['Xolair'],
             'cinqair': ['Cinqair'],
             'dupixent': ['Dupixent', 'Dupi'],
             'repatha': ['repatha'],
             'praluent':['praluent'],
             'nexlitol':['nexlitol','bempedoic acid','ba'],
             'nexlizet':['nexlizet','ezetimibe'],
             'leqvio':['leqvio','inclisiran']
                          }

custom_dict = {'Medicare Std vs LIS': ['Standard vs. LIS', 'STD patients vs LIS patients', 'standard patients and lis patients', 'std patients', 'standard patients', \
                                        'lis patients', 'Medicare patient type'],
               'Medicare Part D Stages': ['part d stage', 'patients by stage', 'patients who reach donut hole', 'patients who reach catastrophic stage', 'initial coverage limit', 'donut hole', 'catastrophic phase',
                                           'Part D phase', 'part D by phase', 'patient progression by stage']}


timeperiod_dict = {
    'ytd': ['YTD', 'this year'],
    '2020': ["2020","'20", "'2020"],
    '2021': ["2021","'21", "'2021"],
    '2022': ["2022","'22", "'2022"],
    '2023': ["2023","'23", "'2023"],
    '2024': ["2024","'24", "'2024"],
    '2025': ["2025","'25", "'2025"],
    'r4y': ['last 4 years', 'r4y', 'last 4 years', 'last four years'],
    'r24m': ['last 24 months', 'r24', 'r24m', 'last 2 years', 'last two years'],
    'r18m': ['last 18 months', 'r18', 'r18m', 'most recent 18 months', 'recent 18 months'],
    'r17m': ['last 17 months', 'r17', 'r17m', 'most recent 17 months', 'recent 17 months'],
    'r16m': ['last 16 months', 'r16', 'r16m', 'most recent 16 months', 'recent 16 months'],
    'r15m': ['last 15 months', 'r15', 'r15m', 'most recent 15 months', 'recent 15 months'],
    'r14m': ['last 14 months', 'r14', 'r14m', 'most recent 14 months', 'recent 14 months'],
    'r13m': ['last 13 months', 'r13', 'r13m', 'most recent 13 months', 'recent 13 months'],
    'r12m': ['last 12 months', 'r12', 'r12m', 'most recent 12 months', 'recent 12 months'],
    'r11m': ['last 11 months', 'r11', 'r11m', 'most recent 11 months', 'recent 11 months'],
    'r10m': ['last 10 months', 'r10', 'r10m', 'most recent 10 months', 'recent 10 months'],
    'r9m': ['last 9 months', 'r9', 'r9m', 'most recent 9 months', 'recent 9 months'],
    'r8m': ['last 8 months', 'r8', 'r8m', 'most recent 8 months', 'recent 8 months'],
    'r6m': ['last 6 months', 'r6', 'r6m', 'most recent 6 months', 'recent 6 months'],
    'r7m': ['last 7 months', 'r7', 'r7m', 'most recent 7 months', 'recent 7 months'],
    'r5m': ['last 5 months', 'r5', 'r5m', 'most recent 5 months', 'recent 5 months'],
    'r4m': ['last 4 months', 'r4', 'r4m', 'most recent 4 months', 'recent 4 months'],
    'r3m': ['last 3 months', 'r3', 'r3m', 'most recent 3 months', 'recent 3 months'],
    'r13w': ['last 13 weeks', 'r13w', '13x13'],
    'r2m' : ['last 2 months', 'r2', 'r2m', 'most recent 2 months', 'recent 2 months'],
    'r1m': ['recent month', 'latest month', 'most recent month'],
    'r1q': ['recent quarter', 'latest quarter', 'most recent quarter'],
    'r2q': ['last 2 quarters', 'last 2 qtrs', 'most recent 2 qtrs', 'most recent two qtrs'],
    'r3q': ['last 3 quarters', 'last 3 qtrs', 'most recent 3 qtrs', 'most recent three qtrs'],
    'r4q': ['last 4 quarters', 'last 4 qtrs', 'most recent 4 qtrs', 'most recent four qtrs'],
    'r5q': ['last 5 quarters', 'last 5 qtrs', 'most recent 5 qtrs', 'most recent five qtrs'],
    'r6q': ['last 6 quarters', 'last 6 qtrs', 'most recent 6 qtrs', 'most recent six qtrs'],
    'r7q': ['last 7 quarters', 'last 7 qtrs', 'most recent 7 qtrs', 'most recent seven qtrs'],
    'r8q': ['last 8 quarters', 'last 8 qtrs', 'most recent 8 qtrs', 'most recent eight qtrs'],
    'r9q': ['last 9 quarters', 'last 9 qtrs', 'most recent 9 qtrs', 'most recent nine qtrs'],
    'r10q': ['last 10 quarters', 'last 10 qtrs', 'most recent 10 qtrs', 'most recent ten qtrs'],
    'r11q': ['last 11 quarters', 'last 11 qtrs', 'most recent 11 qtrs', 'most recent eleven qtrs'],
    'r12q': ['last 12 quarters', 'last 12 qtrs', 'most recent 12 qtrs', 'most recent twelve qtrs'],
    'p24m': ['previous 24 months', 'r24', 'r24m', 'last 2 years', 'last two years'],
    'p18m': ['previous 18 months', 'p18', 'p18m'],
    'p17m': ['previous 17 months', 'p17', 'p17m'],
    'p16m': ['previous 16 months', 'p16', 'p16m'],
    'p15m': ['previous 15 months', 'p15', 'p15m'],
    'p14m': ['previous 14 months', 'p14', 'p14m'],
    'p13m': ['previous 13 months', 'p13', 'p13m'],
    'p12m': ['previous 12 months', 'p12', 'p12m'],
    'p11m': ['previous 11 months', 'p11', 'p11m'],
    'p10m': ['previous 10 months', 'p10', 'p10m'],
    'p9m': ['previous 9 months', 'p9', 'p9m',],
    'p8m': ['previous 8 months', 'p8', 'p8m'],
    'p6m': ['previous 6 months', 'p6', 'p6m'],
    'p7m': ['previous 7 months', 'p7', 'p7m'],
    'p5m': ['previous 5 months', 'p5', 'p5m'],
    'p4m': ['previous 4 months', 'p4', 'p4m'],
    'p3m': ['previous 3 months', 'p3', 'p3m'],
    'p13w': ['previous 13 weeks', 'p13w', '13x13'],
    'p2m' : ['previous 2 months', 'p2', 'p2m'],
    'p1m': ['previous month', 'previous month'],
    'p1q': ['previous quarter', 'last quarter'],
    'p2q': ['previous 2 quarters', 'previous 2 qtrs'],
    'p3q': ['previous 3 quarters', 'previous 3 qtrs'],
    'p4q': ['previous 4 quarters', 'previous 4 qtrs'],
    'p5q': ['previous 5 quarters', 'previous 5 qtrs'],
    'p6q': ['previous 6 quarters', 'previous 6 qtrs'],
    'p7q': ['previous 7 quarters', 'previous 7 qtrs'],
    'p8q': ['previous 8 quarters', 'previous 8 qtrs'],
    'p9q': ['previous 9 quarters', 'previous 9 qtrs'],
    'p10q': ['previous 10 quarters', 'previous 10 qtrs'],
    'p11q': ['previous 11 quarters', 'previous 11 qtrs'],
    'p12q': ['previous 12 quarters', 'previous 12 qtrs']

}

timeperiod_filters = {'laad': {'all':
                            {"default": "month_rank_colname <=12", "ytd": "year_rank_colname =  <=1", "r12m": "month_rank_colname <=12", "r24m": "month_rank_colname <=24","r3m": "month_rank_colname <=3", "r6m": "month_rank_colname <=6","r2m": "month_rank_colname <=2","r7m": "month_rank_colname <=7","r4m": "month_rank_colname <=4","r5m": "month_rank_colname <=5","r8m": "month_rank_colname <=8","r9m": "month_rank_colname <=9","r10m": "month_rank_colname <=10","r11m": "month_rank_colname <=11","r13m": "month_rank_colname <=13","r15m": "month_rank_colname <=15","r14m": "month_rank_colname <=14","r16m": "month_rank_colname <=16","r17m": "month_rank_colname <=17","r18m": "month_rank_colname <=18",
                            "r4q": "quarter_rank_colname <=4", "r8q": "quarter_rank_colname <=8", "r12q": "quarter_rank_colname <=12", "r1q": "quarter_rank_colname <=1","r2q": "quarter_rank_colname <=2","r3q": "quarter_rank_colname <=3","r4y": "year_rank_colname <=4",
                            "r5q": "quarter_rank_colname <=5","r6q": "quarter_rank_colname <=6","r7q": "quarter_rank_colname <=7","r9q": "quarter_rank_colname <=9","r10q": "quarter_rank_colname <=10","r11q": "quarter_rank_colname <=11",
                            "p12m": "month_rank_colname <=24 and month_rank_colname >12", "p24m": "month_rank_colname <=36 and month_rank_colname >12","p3m": "month_rank_colname <=6 and month_rank_colname >3", "p6m": "month_rank_colname <=12 and month_rank_colname >6","p2m": "month_rank_colname <=4 and month_rank_colname >2","p7m": "month_rank_colname <=14 and month_rank_colname >7","p4m": "month_rank_colname <=8 and month_rank_colname >4","p5m": "month_rank_colname <=10 and month_rank_colname >5","p8m": "month_rank_colname <=16 and month_rank_colname >8","p9m": "month_rank_colname <=18 and month_rank_colname >9","p10m": "month_rank_colname <=20 and month_rank_colname >10","p11m": "month_rank_colname <=22 and month_rank_colname >11","p13m": "month_rank_colname <=26 and month_rank_colname >13","p15m": "month_rank_colname <=30 and month_rank_colname >15","p14m": "month_rank_colname <=28 and month_rank_colname >14","p16m": "month_rank_colname <=32 and month_rank_colname >16","p17m": "month_rank_colname <=34 and month_rank_colname >17","p18m": "month_rank_colname <=36 and month_rank_colname >18",
                            "p4q": "quarter_rank_colname <=8 and quarter_rank_colname >4", "p8q": "quarter_rank_colname <=16 and quarter_rank_colname >8", "p12q": "quarter_rank_colname <=24 and quarter_rank_colname >12 ", "p1q": "quarter_rank_colname <=2 and quarter_rank_colname >1","p2q": "quarter_rank_colname <=4 and quarter_rank_colname >2","p3q": "quarter_rank_colname <=6 and quarter_rank_colname >3",
                            "p5q": "quarter_rank_colname <=10 and quarter_rank_colname >5","p6q": "quarter_rank_colname <=12 and quarter_rank_colname >6","p7q": "quarter_rank_colname <=14 and quarter_rank_colname >7","p9q": "quarter_rank_colname <=18 and quarter_rank_colname >9","p10q": "quarter_rank_colname <=20 and quarter_rank_colname >10","p11q": "quarter_rank_colname <=22 and quarter_rank_colname 11",
                           "r1y": "year_rank_colname <=1","r2y": "year_rank_colname <=2", "r3y": "year_rank_colname <=3" }},
                      'laad_weekly': {'all':
                            {"default": "month_rank_colname <=12", "ytd": "year_rank_colname =  <=1", "r12m": "month_rank_colname <=12", "r24m": "month_rank_colname <=24","r3m": "month_rank_colname <=3", "r6m": "month_rank_colname <=6","r2m": "month_rank_colname <=2","r7m": "month_rank_colname <=7","r4m": "month_rank_colname <=4","r5m": "month_rank_colname <=5","r8m": "month_rank_colname <=8","r9m": "month_rank_colname <=9","r10m": "month_rank_colname <=10","r11m": "month_rank_colname <=11","r13m": "month_rank_colname <=13","r15m": "month_rank_colname <=15","r14m": "month_rank_colname <=14","r16m": "month_rank_colname <=16","r17m": "month_rank_colname <=17","r18m": "month_rank_colname <=18",
                            "r4q": "quarter_rank_colname <=4", "r8q": "quarter_rank_colname <=8", "r12q": "quarter_rank_colname <=12", "r1q": "quarter_rank_colname <=1","r2q": "quarter_rank_colname <=2","r3q": "quarter_rank_colname <=3",
                            "r5q": "quarter_rank_colname <=5","r6q": "quarter_rank_colname <=6","r7q": "quarter_rank_colname <=7","r9q": "quarter_rank_colname <=9","r10q": "quarter_rank_colname <=10","r11q": "quarter_rank_colname <=11",
                            "p12m": "month_rank_colname <=24 and month_rank_colname >12", "p24m": "month_rank_colname <=36 and month_rank_colname >12","p3m": "month_rank_colname <=6 and month_rank_colname >3", "p6m": "month_rank_colname <=12 and month_rank_colname >6","p2m": "month_rank_colname <=4 and month_rank_colname >2","p7m": "month_rank_colname <=14 and month_rank_colname >7","p4m": "month_rank_colname <=8 and month_rank_colname >4","p5m": "month_rank_colname <=10 and month_rank_colname >5","p8m": "month_rank_colname <=16 and month_rank_colname >8","p9m": "month_rank_colname <=18 and month_rank_colname >9","p10m": "month_rank_colname <=20 and month_rank_colname >10","p11m": "month_rank_colname <=22 and month_rank_colname >11","p13m": "month_rank_colname <=26 and month_rank_colname >13","p15m": "month_rank_colname <=30 and month_rank_colname >15","p14m": "month_rank_colname <=28 and month_rank_colname >14","p16m": "month_rank_colname <=32 and month_rank_colname >16","p17m": "month_rank_colname <=34 and month_rank_colname >17","p18m": "month_rank_colname <=36 and month_rank_colname >18",
                            "p4q": "quarter_rank_colname <=8 and quarter_rank_colname >4", "p8q": "quarter_rank_colname <=16 and quarter_rank_colname >8", "p12q": "quarter_rank_colname <=24 and quarter_rank_colname >12 ", "p1q": "quarter_rank_colname <=2 and quarter_rank_colname >1","p2q": "quarter_rank_colname <=4 and quarter_rank_colname >2","p3q": "quarter_rank_colname <=6 and quarter_rank_colname >3",
                            "p5q": "quarter_rank_colname <=10 and quarter_rank_colname >5","p6q": "quarter_rank_colname <=12 and quarter_rank_colname >6","p7q": "quarter_rank_colname <=14 and quarter_rank_colname >7","p9q": "quarter_rank_colname <=18 and quarter_rank_colname >9","p10q": "quarter_rank_colname <=20 and quarter_rank_colname >10","p11q": "quarter_rank_colname <=22 and quarter_rank_colname 11",
                           "r1y": "year_rank_colname <=1","r2y": "year_rank_colname <=2", "r3y": "year_rank_colname <=3" }},
                      'clinformatics': {'all':
                            {"default": "month_rank_colname <=12", "ytd": "year_rank_colname =  <=1", "r12m": "month_rank_colname <=12", "r24m": "month_rank_colname <=24","r3m": "month_rank_colname <=3", "r6m": "month_rank_colname <=6","r2m": "month_rank_colname <=2","r7m": "month_rank_colname <=7","r4m": "month_rank_colname <=4","r5m": "month_rank_colname <=5","r8m": "month_rank_colname <=8","r9m": "month_rank_colname <=9","r10m": "month_rank_colname <=10","r11m": "month_rank_colname <=11","r13m": "month_rank_colname <=13","r15m": "month_rank_colname <=15","r14m": "month_rank_colname <=14","r16m": "month_rank_colname <=16","r17m": "month_rank_colname <=17","r18m": "month_rank_colname <=18",
                            "r4q": "quarter_rank_colname <=4", "r8q": "quarter_rank_colname <=8", "r12q": "quarter_rank_colname <=12", "r1q": "quarter_rank_colname <=1","r2q": "quarter_rank_colname <=2","r3q": "quarter_rank_colname <=3",
                            "r5q": "quarter_rank_colname <=5","r6q": "quarter_rank_colname <=6","r7q": "quarter_rank_colname <=7","r9q": "quarter_rank_colname <=9","r10q": "quarter_rank_colname <=10","r11q": "quarter_rank_colname <=11",
                            "p12m": "month_rank_colname <=24 and month_rank_colname >12", "p24m": "month_rank_colname <=36 and month_rank_colname >12","p3m": "month_rank_colname <=6 and month_rank_colname >3", "p6m": "month_rank_colname <=12 and month_rank_colname >6","p2m": "month_rank_colname <=4 and month_rank_colname >2","p7m": "month_rank_colname <=14 and month_rank_colname >7","p4m": "month_rank_colname <=8 and month_rank_colname >4","p5m": "month_rank_colname <=10 and month_rank_colname >5","p8m": "month_rank_colname <=16 and month_rank_colname >8","p9m": "month_rank_colname <=18 and month_rank_colname >9","p10m": "month_rank_colname <=20 and month_rank_colname >10","p11m": "month_rank_colname <=22 and month_rank_colname >11","p13m": "month_rank_colname <=26 and month_rank_colname >13","p15m": "month_rank_colname <=30 and month_rank_colname >15","p14m": "month_rank_colname <=28 and month_rank_colname >14","p16m": "month_rank_colname <=32 and month_rank_colname >16","p17m": "month_rank_colname <=34 and month_rank_colname >17","p18m": "month_rank_colname <=36 and month_rank_colname >18",
                            "p4q": "quarter_rank_colname <=8 and quarter_rank_colname >4", "p8q": "quarter_rank_colname <=16 and quarter_rank_colname >8", "p12q": "quarter_rank_colname <=24 and quarter_rank_colname >12 ", "p1q": "quarter_rank_colname <=2 and quarter_rank_colname >1","p2q": "quarter_rank_colname <=4 and quarter_rank_colname >2","p3q": "quarter_rank_colname <=6 and quarter_rank_colname >3",
                            "p5q": "quarter_rank_colname <=10 and quarter_rank_colname >5","p6q": "quarter_rank_colname <=12 and quarter_rank_colname >6","p7q": "quarter_rank_colname <=14 and quarter_rank_colname >7","p9q": "quarter_rank_colname <=18 and quarter_rank_colname >9","p10q": "quarter_rank_colname <=20 and quarter_rank_colname >10","p11q": "quarter_rank_colname <=22 and quarter_rank_colname 11",
                           "r1y": "year_rank_colname <=1","r2y": "year_rank_colname <=2", "r3y": "year_rank_colname <=3"}},
                      'mmit': {'all':
                            {"default": "month_rank_colname <=12", "ytd": "year_rank_colname =  <=1", "r12m": "month_rank_colname <=12", "r24m": "month_rank_colname <=24","r3m": "month_rank_colname <=3", "r6m": "month_rank_colname <=6","r2m": "month_rank_colname <=2","r7m": "month_rank_colname <=7","r4m": "month_rank_colname <=4","r5m": "month_rank_colname <=5","r8m": "month_rank_colname <=8","r9m": "month_rank_colname <=9","r10m": "month_rank_colname <=10","r11m": "month_rank_colname <=11","r13m": "month_rank_colname <=13","r15m": "month_rank_colname <=15","r14m": "month_rank_colname <=14","r16m": "month_rank_colname <=16","r17m": "month_rank_colname <=17","r18m": "month_rank_colname <=18",
                            "r4q": "quarter_rank_colname <=4", "r8q": "quarter_rank_colname <=8", "r12q": "quarter_rank_colname <=12", "r1q": "quarter_rank_colname <=1","r2q": "quarter_rank_colname <=2","r3q": "quarter_rank_colname <=3",
                            "r5q": "quarter_rank_colname <=5","r6q": "quarter_rank_colname <=6","r7q": "quarter_rank_colname <=7","r9q": "quarter_rank_colname <=9","r10q": "quarter_rank_colname <=10","r11q": "quarter_rank_colname <=11",
                            "p12m": "month_rank_colname <=24 and month_rank_colname >12", "p24m": "month_rank_colname <=36 and month_rank_colname >12","p3m": "month_rank_colname <=6 and month_rank_colname >3", "p6m": "month_rank_colname <=12 and month_rank_colname >6","p2m": "month_rank_colname <=4 and month_rank_colname >2","p7m": "month_rank_colname <=14 and month_rank_colname >7","p4m": "month_rank_colname <=8 and month_rank_colname >4","p5m": "month_rank_colname <=10 and month_rank_colname >5","p8m": "month_rank_colname <=16 and month_rank_colname >8","p9m": "month_rank_colname <=18 and month_rank_colname >9","p10m": "month_rank_colname <=20 and month_rank_colname >10","p11m": "month_rank_colname <=22 and month_rank_colname >11","p13m": "month_rank_colname <=26 and month_rank_colname >13","p15m": "month_rank_colname <=30 and month_rank_colname >15","p14m": "month_rank_colname <=28 and month_rank_colname >14","p16m": "month_rank_colname <=32 and month_rank_colname >16","p17m": "month_rank_colname <=34 and month_rank_colname >17","p18m": "month_rank_colname <=36 and month_rank_colname >18",
                            "p4q": "quarter_rank_colname <=8 and quarter_rank_colname >4", "p8q": "quarter_rank_colname <=16 and quarter_rank_colname >8", "p12q": "quarter_rank_colname <=24 and quarter_rank_colname >12 ", "p1q": "quarter_rank_colname <=2 and quarter_rank_colname >1","p2q": "quarter_rank_colname <=4 and quarter_rank_colname >2","p3q": "quarter_rank_colname <=6 and quarter_rank_colname >3",
                            "p5q": "quarter_rank_colname <=10 and quarter_rank_colname >5","p6q": "quarter_rank_colname <=12 and quarter_rank_colname >6","p7q": "quarter_rank_colname <=14 and quarter_rank_colname >7","p9q": "quarter_rank_colname <=18 and quarter_rank_colname >9","p10q": "quarter_rank_colname <=20 and quarter_rank_colname >10","p11q": "quarter_rank_colname <=22 and quarter_rank_colname 11",
                           "r1y": "year_rank_colname <=1","r2y": "year_rank_colname <=2", "r3y": "year_rank_colname <=3" }},
                      'mmit_change': {'all':
                            {"default": "month_rank_colname <=12", "ytd": "year_rank_colname =  <=1", "r12m": "month_rank_colname <=12", "r24m": "month_rank_colname <=24","r3m": "month_rank_colname <=3", "r6m": "month_rank_colname <=6","r2m": "month_rank_colname <=2","r7m": "month_rank_colname <=7","r4m": "month_rank_colname <=4","r5m": "month_rank_colname <=5","r8m": "month_rank_colname <=8","r9m": "month_rank_colname <=9","r10m": "month_rank_colname <=10","r11m": "month_rank_colname <=11","r13m": "month_rank_colname <=13","r15m": "month_rank_colname <=15","r14m": "month_rank_colname <=14","r16m": "month_rank_colname <=16","r17m": "month_rank_colname <=17","r18m": "month_rank_colname <=18",
                            "r4q": "quarter_rank_colname <=4", "r8q": "quarter_rank_colname <=8", "r12q": "quarter_rank_colname <=12", "r1q": "quarter_rank_colname <=1","r2q": "quarter_rank_colname <=2","r3q": "quarter_rank_colname <=3",
                            "r5q": "quarter_rank_colname <=5","r6q": "quarter_rank_colname <=6","r7q": "quarter_rank_colname <=7","r9q": "quarter_rank_colname <=9","r10q": "quarter_rank_colname <=10","r11q": "quarter_rank_colname <=11",
                            "p12m": "month_rank_colname <=24 and month_rank_colname >12", "p24m": "month_rank_colname <=36 and month_rank_colname >12","p3m": "month_rank_colname <=6 and month_rank_colname >3", "p6m": "month_rank_colname <=12 and month_rank_colname >6","p2m": "month_rank_colname <=4 and month_rank_colname >2","p7m": "month_rank_colname <=14 and month_rank_colname >7","p4m": "month_rank_colname <=8 and month_rank_colname >4","p5m": "month_rank_colname <=10 and month_rank_colname >5","p8m": "month_rank_colname <=16 and month_rank_colname >8","p9m": "month_rank_colname <=18 and month_rank_colname >9","p10m": "month_rank_colname <=20 and month_rank_colname >10","p11m": "month_rank_colname <=22 and month_rank_colname >11","p13m": "month_rank_colname <=26 and month_rank_colname >13","p15m": "month_rank_colname <=30 and month_rank_colname >15","p14m": "month_rank_colname <=28 and month_rank_colname >14","p16m": "month_rank_colname <=32 and month_rank_colname >16","p17m": "month_rank_colname <=34 and month_rank_colname >17","p18m": "month_rank_colname <=36 and month_rank_colname >18",
                            "p4q": "quarter_rank_colname <=8 and quarter_rank_colname >4", "p8q": "quarter_rank_colname <=16 and quarter_rank_colname >8", "p12q": "quarter_rank_colname <=24 and quarter_rank_colname >12 ", "p1q": "quarter_rank_colname <=2 and quarter_rank_colname >1","p2q": "quarter_rank_colname <=4 and quarter_rank_colname >2","p3q": "quarter_rank_colname <=6 and quarter_rank_colname >3",
                            "p5q": "quarter_rank_colname <=10 and quarter_rank_colname >5","p6q": "quarter_rank_colname <=12 and quarter_rank_colname >6","p7q": "quarter_rank_colname <=14 and quarter_rank_colname >7","p9q": "quarter_rank_colname <=18 and quarter_rank_colname >9","p10q": "quarter_rank_colname <=20 and quarter_rank_colname >10","p11q": "quarter_rank_colname <=22 and quarter_rank_colname 11",
                           "r1y": "year_rank_colname <=1","r2y": "year_rank_colname <=2", "r3y": "year_rank_colname <=3" }},
                        'npa': {'all':
                            {"default": "month_rank_colname <=12", "ytd": "year_rank_colname =  <=1", "r12m": "month_rank_colname <=12", "r24m": "month_rank_colname <=24","r3m": "month_rank_colname <=3", "r6m": "month_rank_colname <=6","r2m": "month_rank_colname <=2","r7m": "month_rank_colname <=7","r4m": "month_rank_colname <=4","r5m": "month_rank_colname <=5","r8m": "month_rank_colname <=8","r9m": "month_rank_colname <=9","r10m": "month_rank_colname <=10","r11m": "month_rank_colname <=11","r13m": "month_rank_colname <=13","r15m": "month_rank_colname <=15","r14m": "month_rank_colname <=14","r16m": "month_rank_colname <=16","r17m": "month_rank_colname <=17","r18m": "month_rank_colname <=18",
                            "r4q": "quarter_rank_colname <=4", "r8q": "quarter_rank_colname <=8", "r12q": "quarter_rank_colname <=12", "r1q": "quarter_rank_colname <=1","r2q": "quarter_rank_colname <=2","r3q": "quarter_rank_colname <=3",
                            "r5q": "quarter_rank_colname <=5","r6q": "quarter_rank_colname <=6","r7q": "quarter_rank_colname <=7","r9q": "quarter_rank_colname <=9","r10q": "quarter_rank_colname <=10","r11q": "quarter_rank_colname <=11",
                            "p12m": "month_rank_colname <=24 and month_rank_colname >12", "p24m": "month_rank_colname <=36 and month_rank_colname >12","p3m": "month_rank_colname <=6 and month_rank_colname >3", "p6m": "month_rank_colname <=12 and month_rank_colname >6","p2m": "month_rank_colname <=4 and month_rank_colname >2","p7m": "month_rank_colname <=14 and month_rank_colname >7","p4m": "month_rank_colname <=8 and month_rank_colname >4","p5m": "month_rank_colname <=10 and month_rank_colname >5","p8m": "month_rank_colname <=16 and month_rank_colname >8","p9m": "month_rank_colname <=18 and month_rank_colname >9","p10m": "month_rank_colname <=20 and month_rank_colname >10","p11m": "month_rank_colname <=22 and month_rank_colname >11","p13m": "month_rank_colname <=26 and month_rank_colname >13","p15m": "month_rank_colname <=30 and month_rank_colname >15","p14m": "month_rank_colname <=28 and month_rank_colname >14","p16m": "month_rank_colname <=32 and month_rank_colname >16","p17m": "month_rank_colname <=34 and month_rank_colname >17","p18m": "month_rank_colname <=36 and month_rank_colname >18",
                            "p4q": "quarter_rank_colname <=8 and quarter_rank_colname >4", "p8q": "quarter_rank_colname <=16 and quarter_rank_colname >8", "p12q": "quarter_rank_colname <=24 and quarter_rank_colname >12 ", "p1q": "quarter_rank_colname <=2 and quarter_rank_colname >1","p2q": "quarter_rank_colname <=4 and quarter_rank_colname >2","p3q": "quarter_rank_colname <=6 and quarter_rank_colname >3",
                            "p5q": "quarter_rank_colname <=10 and quarter_rank_colname >5","p6q": "quarter_rank_colname <=12 and quarter_rank_colname >6","p7q": "quarter_rank_colname <=14 and quarter_rank_colname >7","p9q": "quarter_rank_colname <=18 and quarter_rank_colname >9","p10q": "quarter_rank_colname <=20 and quarter_rank_colname >10","p11q": "quarter_rank_colname <=22 and quarter_rank_colname 11",
                           "r1y": "year_rank_colname <=1","r2y": "year_rank_colname <=2", "r3y": "year_rank_colname <=3" }},
                        'emisar': {'all':
                            {"default": "month_rank_colname <=12", "ytd": "year_rank_colname =  <=1", "r12m": "month_rank_colname <=12", "r24m": "month_rank_colname <=24","r3m": "month_rank_colname <=3", "r6m": "month_rank_colname <=6","r2m": "month_rank_colname <=2","r7m": "month_rank_colname <=7","r4m": "month_rank_colname <=4","r5m": "month_rank_colname <=5","r8m": "month_rank_colname <=8","r9m": "month_rank_colname <=9","r10m": "month_rank_colname <=10","r11m": "month_rank_colname <=11","r13m": "month_rank_colname <=13","r15m": "month_rank_colname <=15","r14m": "month_rank_colname <=14","r16m": "month_rank_colname <=16","r17m": "month_rank_colname <=17","r18m": "month_rank_colname <=18",
                            "r4q": "quarter_rank_colname <=4", "r8q": "quarter_rank_colname <=8", "r12q": "quarter_rank_colname <=12", "r1q": "quarter_rank_colname <=1","r2q": "quarter_rank_colname <=2","r3q": "quarter_rank_colname <=3",
                            "r5q": "quarter_rank_colname <=5","r6q": "quarter_rank_colname <=6","r7q": "quarter_rank_colname <=7","r9q": "quarter_rank_colname <=9","r10q": "quarter_rank_colname <=10","r11q": "quarter_rank_colname <=11",
                            "p12m": "month_rank_colname <=24 and month_rank_colname >12", "p24m": "month_rank_colname <=36 and month_rank_colname >12","p3m": "month_rank_colname <=6 and month_rank_colname >3", "p6m": "month_rank_colname <=12 and month_rank_colname >6","p2m": "month_rank_colname <=4 and month_rank_colname >2","p7m": "month_rank_colname <=14 and month_rank_colname >7","p4m": "month_rank_colname <=8 and month_rank_colname >4","p5m": "month_rank_colname <=10 and month_rank_colname >5","p8m": "month_rank_colname <=16 and month_rank_colname >8","p9m": "month_rank_colname <=18 and month_rank_colname >9","p10m": "month_rank_colname <=20 and month_rank_colname >10","p11m": "month_rank_colname <=22 and month_rank_colname >11","p13m": "month_rank_colname <=26 and month_rank_colname >13","p15m": "month_rank_colname <=30 and month_rank_colname >15","p14m": "month_rank_colname <=28 and month_rank_colname >14","p16m": "month_rank_colname <=32 and month_rank_colname >16","p17m": "month_rank_colname <=34 and month_rank_colname >17","p18m": "month_rank_colname <=36 and month_rank_colname >18",
                            "p4q": "quarter_rank_colname <=8 and quarter_rank_colname >4", "p8q": "quarter_rank_colname <=16 and quarter_rank_colname >8", "p12q": "quarter_rank_colname <=24 and quarter_rank_colname >12 ", "p1q": "quarter_rank_colname <=2 and quarter_rank_colname >1","p2q": "quarter_rank_colname <=4 and quarter_rank_colname >2","p3q": "quarter_rank_colname <=6 and quarter_rank_colname >3",
                            "p5q": "quarter_rank_colname <=10 and quarter_rank_colname >5","p6q": "quarter_rank_colname <=12 and quarter_rank_colname >6","p7q": "quarter_rank_colname <=14 and quarter_rank_colname >7","p9q": "quarter_rank_colname <=18 and quarter_rank_colname >9","p10q": "quarter_rank_colname <=20 and quarter_rank_colname >10","p11q": "quarter_rank_colname <=22 and quarter_rank_colname 11",
                           "r1y": "year_rank_colname <=1","r2y": "year_rank_colname <=2", "r3y": "year_rank_colname <=3" }},
                        'copay': {'all':
                            {"default": "month_rank_colname <=12", "ytd": "year_rank_colname =  <=1", "r12m": "month_rank_colname <=12", "r24m": "month_rank_colname <=24","r3m": "month_rank_colname <=3", "r6m": "month_rank_colname <=6","r2m": "month_rank_colname <=2","r7m": "month_rank_colname <=7","r4m": "month_rank_colname <=4","r5m": "month_rank_colname <=5","r8m": "month_rank_colname <=8","r9m": "month_rank_colname <=9","r10m": "month_rank_colname <=10","r11m": "month_rank_colname <=11","r13m": "month_rank_colname <=13","r15m": "month_rank_colname <=15","r14m": "month_rank_colname <=14","r16m": "month_rank_colname <=16","r17m": "month_rank_colname <=17","r18m": "month_rank_colname <=18",
                            "r4q": "quarter_rank_colname <=4", "r8q": "quarter_rank_colname <=8", "r12q": "quarter_rank_colname <=12", "r1q": "quarter_rank_colname <=1","r2q": "quarter_rank_colname <=2","r3q": "quarter_rank_colname <=3",
                            "r5q": "quarter_rank_colname <=5","r6q": "quarter_rank_colname <=6","r7q": "quarter_rank_colname <=7","r9q": "quarter_rank_colname <=9","r10q": "quarter_rank_colname <=10","r11q": "quarter_rank_colname <=11",
                            "p12m": "month_rank_colname <=24 and month_rank_colname >12", "p24m": "month_rank_colname <=36 and month_rank_colname >12","p3m": "month_rank_colname <=6 and month_rank_colname >3", "p6m": "month_rank_colname <=12 and month_rank_colname >6","p2m": "month_rank_colname <=4 and month_rank_colname >2","p7m": "month_rank_colname <=14 and month_rank_colname >7","p4m": "month_rank_colname <=8 and month_rank_colname >4","p5m": "month_rank_colname <=10 and month_rank_colname >5","p8m": "month_rank_colname <=16 and month_rank_colname >8","p9m": "month_rank_colname <=18 and month_rank_colname >9","p10m": "month_rank_colname <=20 and month_rank_colname >10","p11m": "month_rank_colname <=22 and month_rank_colname >11","p13m": "month_rank_colname <=26 and month_rank_colname >13","p15m": "month_rank_colname <=30 and month_rank_colname >15","p14m": "month_rank_colname <=28 and month_rank_colname >14","p16m": "month_rank_colname <=32 and month_rank_colname >16","p17m": "month_rank_colname <=34 and month_rank_colname >17","p18m": "month_rank_colname <=36 and month_rank_colname >18",
                            "p4q": "quarter_rank_colname <=8 and quarter_rank_colname >4", "p8q": "quarter_rank_colname <=16 and quarter_rank_colname >8", "p12q": "quarter_rank_colname <=24 and quarter_rank_colname >12 ", "p1q": "quarter_rank_colname <=2 and quarter_rank_colname >1","p2q": "quarter_rank_colname <=4 and quarter_rank_colname >2","p3q": "quarter_rank_colname <=6 and quarter_rank_colname >3",
                            "p5q": "quarter_rank_colname <=10 and quarter_rank_colname >5","p6q": "quarter_rank_colname <=12 and quarter_rank_colname >6","p7q": "quarter_rank_colname <=14 and quarter_rank_colname >7","p9q": "quarter_rank_colname <=18 and quarter_rank_colname >9","p10q": "quarter_rank_colname <=20 and quarter_rank_colname >10","p11q": "quarter_rank_colname <=22 and quarter_rank_colname 11",
                           "r1y": "year_rank_colname <=1","r2y": "year_rank_colname <=2", "r3y": "year_rank_colname <=3"}}
                      }



channel_dict = {'Commercial': ['Commercial', 'Third Party', 'Commercial channel', 'Comm'],
                'Medicare': ['Medicare', 'Medicare channel'],
                'Medicare Part D': ['Medicare Part D', 'Part D'],
                'PDP': ['PDP'],
                'MAPD': ['Medicare Advantage Part D', 'MAPD'],
                'Medicare Advantage': ['Medicare Advantage', 'Medicare Advantage Channel'],
                'Medicare FFS': ['Medicare FFS', 'FFS Medicare', 'MAC B', 'MAC'],
                'Medicaid': ['Medicaid'],
                'State Medicaid': ['State Medicaid', 'FFS Medicaid', 'Medicaid FFS'],
                'Managed Medicaid': ['Managed Medicaid'],
                'All channels': ['All channels','all method of payment', 'all payment types']
                }
indication_dict = {'SA': ['Severe Asthma', 'SA', 'asthma'],
             'RA':['Rheumatoid Arthritis','RA Indication','RA'],
'PSO':['Psoriasis','PsO Indication','PsO'],
'PSA':['Psoriatic Arthritis','PsA Indication','PsA'],
'CD':['Crohn','Crohns Disease','CD Indication','Chrons'],
'AS':['ankylosing spondylitis','AS Indication'],
'UC':['ulcerative colitis','US Indication'],
'HS':['Hidradenitis suppurativa','HS Indication'],
'JRA':['juvenile ra','juvenile rheumatoid arthritis','JRA Indication'],
'SPONDYLOARTHRITIS':['SPONDYLOARTHRITIS'],
'POTENTIAL AAV':['POTENTIAL AAV'],
'WEGENERS GRANULOMATOSIS [GPA]':['GPA Indication','WEGENERS GRANULOMATOSIS','WEGENERS GRANULOMATOSIS [GPA]'],
'MYASTHENIA GRAVIS (MG)':['MG Indication','MYASTHENIA GRAVIS ','MYASTHENIA GRAVIS (MG)'],
'MEUROMYELITIS OPTICA (NMOSD)':['MEUROMYELITIS OPTICA','NMOSD Indication','MEUROMYELITIS OPTICA (NMOSD)'],
'HEMOLYTIC-UREMIC SYNDROME (HUS)':['HUS Indication','HEMOLYTIC-UREMIC SYNDROME','HEMOLYTIC-UREMIC SYNDROME (HUS)'],
'CHURG-STRAUSS [EGPA]':['EGPA Indication','CHURG-STRAUSS [EGPA]'],
'MICROSCOPIC POLYANGIITIS':['CHURG-STRAUSS','MICROSCOPIC POLYANGIITIS'],
'PAROXYSMAL NOCTURNAL HEMOGLOBINURIA (PNH)':['PAROXYSMAL NOCTURNAL HEMOGLOBINURIA','PNH Indication','PAROXYSMAL NOCTURNAL HEMOGLOBINURIA (PNH)']}

grouping_dict={'anabolic':['anabolic','anabolic products','anabolics','anabolic drugs','anabolic brands','anabolic therapies'],'testgroup':['test group'],
               
               'advanced therapy':['adv therapy','adv_therapy','advanced therapy','advanced therapies']}

controller_dict = {'United': ['UHG', 'UHC', 'United Health Group', 'United Health', 'United'],
                   'Optum': ['Optum', 'OptumRx'],
                   'Express Scripts': ['Express Scripts Inc', 'ESI', 'Express Scripts'],
                   'Anthem': ['Anthem', 'Anthem Inc'],
                   'bcbsnc': ['BCBS NC', 'Blue Cross Blue Shield of North Carolina', 'BCBS North Carolina'],
                   'CVS': ['CVS', 'Caremark', 'CVS Caremark','CVS HEALTH (AETNA)'],
                   'HAWAII MEDICAL SERVICE ASSOCIATION':['HAWAII MEDICAL SERVICE ASSOCIATION','HMSA'],
                   'CVS HEALTH (AETNA)':['CVS','Aetna','CVS/Aetna','CVS HEALTH (AETNA)','CVS HEALTH'],
                    'OPTUMRX':['Optum','Optum','Optumrx','OPTUMRX'],
                    'EXPRESS SCRIPTS PBM':['ESI','Express','Express Scripts','EXPRESS SCRIPTS PBM'],
                    'UNITEDHEALTH GROUP, INC.':['UHG','United','United Health','UNITEDHEALTH GROUP, INC.'],
                    'HUMANA, INC.':['Humana','HUMANA, INC.'],
                    'CENTENE CORPORATION':['Centene','CENTENE CORPORATION'],
                    'CIGNA CORPORATION':['Cigna','CIGNA CORPORATION'],
                    'HEALTH CARE SERVICE CORPORATION':['HCSC','HEALTH CARE SERVICE CORPORATION'],
                    'STATE OF CALIFORNIA':['California','STATE OF CALIFORNIA'],
                    'BLUE CROSS BLUE SHIELD OF MICHIGAN':['BCBS Michigan','BLUE CROSS BLUE SHIELD OF MICHIGAN'],
                    'ELEVANCE HEALTH, INC.':['Elevance','ELEVANCE HEALTH, INC.'],
                    'HIGHMARK, INC.':['Highmark','HIGHMARK, INC.'],
                    'STATE OF NEW YORK':['NY','New York','STATE OF NEW YORK'],
                    'MEDIMPACT HEALTHCARE SYSTEMS, INC.':['Medimpact','MEDIMPACT HEALTHCARE SYSTEMS, INC.'],
                    'ELIXIR PBM':['Elixir','ELIXIR PBM'],
                    'BLUE CROSS BLUE SHIELD OF NORTH CAROLINA':['BCBS NC','BCBS North Carolina','BLUE CROSS BLUE SHIELD OF NORTH CAROLINA'],
                    'INDEPENDENCE BLUE CROSS':['Independence','BC Independence','INDEPENDENCE BLUE CROSS'],
                    'MOLINA HEALTH CARE, INC.':['Molina','MOLINA HEALTH CARE, INC.'],
                    'HORIZON BLUE CROSS BLUE SHIELD OF NEW JERSEY':['Horizon BCBS NJ','BCBS New Jersey','Horizon BCBS New Jersey','HORIZON BLUE CROSS BLUE SHIELD OF NEW JERSEY'],
                    'STATE OF OHIO':['Ohio','STATE OF OHIO'],
                    'BLUE CROSS BLUE SHIELD OF MINNESOTA':['BCBS MN','BCBS Minnesota','BLUE CROSS BLUE SHIELD OF MINNESOTA'],
                    'BLUE CROSS BLUE SHIELD OF MASSACHUSETTS':['BCBS MA','BCBS Massachusetts','BLUE CROSS BLUE SHIELD OF MASSACHUSETTS'],
                    'NAVITUS HEALTH SOLUTIONS PBM':['Navitus','Navitus Health','NAVITUS HEALTH SOLUTIONS PBM'],
                    'FLORIDA BLUE':['Florida Blue','FLORIDA BLUE'],
                    'STATE OF NORTH CAROLINA':['NC','North Carolina','STATE OF NORTH CAROLINA'],
                    'BLUE CROSS BLUE SHIELD OF ALABAMA':['BCBS AL','BCBS Alabama','BLUE CROSS BLUE SHIELD OF ALABAMA'],
                    'STATE OF MASSACHUSETTS':['Massachusetts','STATE OF MASSACHUSETTS'],
                    'PRIME THERAPEUTICS':['Prime','PRIME THERAPEUTICS'],
                    'LIFETIME HEALTHCARE COMPANIES':['Lifetime','LHC','LIFETIME HEALTHCARE COMPANIES'],
                    'SPECTRUM HEALTH SYSTEM':['Spectrum','SPECTRUM HEALTH SYSTEM'],
                    'POINT32HEALTH':['Point32','POINT32HEALTH'],
                    'PREMERA, INC.':['Premera','PREMERA, INC.'],
                    'STATE OF KENTUCKY':['Kentucky','STATE OF KENTUCKY'],
                    'BLUE CROSS BLUE SHIELD ASSOCIATION CORPORATION':['BCBS Corporation','BLUE CROSS BLUE SHIELD ASSOCIATION CORPORATION'],
                    'COMMONWEALTH OF PUERTO RICO':['Puerto Rico','COMMONWEALTH OF PUERTO RICO'],
                    'SENTARA HEALTH SYSTEM CORPORATION':['Sentara','SENTARA HEALTH SYSTEM CORPORATION'],
                    'DEPARTMENT OF DEFENSE - TRICARE':['DOD Tricare','Tricare','DOD','DEPARTMENT OF DEFENSE - TRICARE'],
                    'BLUE CROSS BLUE SHIELD OF TENNESSEE':['BCBS TN','BCBS Tennessee','BLUE CROSS BLUE SHIELD OF TENNESSEE'],
                    'STATE OF WISCONSIN':['Wisconsin','STATE OF WISCONSIN'],
                    'HEALTHPARTNERS, INC.':['Healthpartners','Health Partners','HEALTHPARTNERS, INC.'],
                    'CAMBIA HEALTH SOLUTIONS':['Cambia','CAMBIA HEALTH SOLUTIONS'],
                    'STATE OF LOUISIANA':['Louisiana','STATE OF LOUISIANA'],
                    'MCS LIFE INSURANCE COMPANY':['MCS','MCS LIFE INSURANCE COMPANY'],
                    'CAREFIRST, INC.':['Carefirst','CAREFIRST, INC.'],
                    'STATE OF COLORADO':['Colorado','STATE OF COLORADO'],
                    'STATE OF FLORIDA':['Florida','STATE OF FLORIDA'],
                    'STATE OF TEXAS':['Texas','STATE OF TEXAS'],
                    'TRIPLE-S, INC.':['TripleS','TRIPLE-S, INC.'],
                    'BLUE CROSS AND BLUE SHIELD OF LOUISIANA':['BCBS LA','BLUE CROSS AND BLUE SHIELD OF LOUISIANA'],
                    'UCARE MINNESOTA, INC.':['Ucare MN','Ucare minnesota','UCARE MINNESOTA, INC.'],
                    'STATE OF TENNESSEE':['Tennessee','STATE OF TENNESSEE'],
                    'BLUE CROSS BLUE SHIELD OF ARIZONA, INC.':['BCBS AZ','BCBS Arizona','BLUE CROSS BLUE SHIELD OF ARIZONA, INC.'],
                    'MEDICAL MUTUAL OF OHIO CORPORATION':['MM Ohio','Medican mutual ohio','MEDICAL MUTUAL OF OHIO CORPORATION'],
                    'STATE OF MISSOURI':['Missouri','STATE OF MISSOURI'],
                    'HEALTH ALLIANCE PLAN HAP (HENRY FORD HEALTH SYSTEM)':['Henry ford','health alliance plan','HAP','HEALTH ALLIANCE PLAN HAP (HENRY FORD HEALTH SYSTEM)'],
                    'CHANGE HEALTHCARE PBM':['Change healthcare','CHANGE HEALTHCARE PBM'],
                    'CAPITAL RX PBM':['Capital rx','Capitalth plan','CAPITAL RX PBM'],
                    'EMBLEMHEALTH, INC.':['Emblem health','Emblem','EMBLEMHEALTH, INC.'],
                    'BLUE CROSS OF IDAHO HEALTH SERVICES, INC.':['BCBS Idaho','BCBS ID','BLUE CROSS OF IDAHO HEALTH SERVICES, INC.'],
                    'STATE OF OKLAHOMA':['Oklahoma','STATE OF OKLAHOMA'],
                    'MCLAREN HEALTH CARE CORPORATION':['Mclaren','MCLAREN HEALTH CARE CORPORATION'],
                    'HAWAII MEDICAL SERVICE ASSOCIATION':['Hawaii Medical','HMSA','HAWAII MEDICAL SERVICE ASSOCIATION'],
                    'STATE OF PENNSYLVANIA':['Pennsylvania','STATE OF PENNSYLVANIA'],
                    'OSCAR INSURANCE':['Oscar','OSCAR INSURANCE'],
                    'WELLMARK, INC.':['Wellmark','WELLMARK, INC.'],
                    'MEDICA HEALTH PLANS':['Medica','Medica health','MEDICA HEALTH PLANS'],
                    'BLUE CROSS BLUE SHIELD OF MISSISSIPPI CORPORATION':['BCBS MS','BCBS Mississippi','BLUE CROSS BLUE SHIELD OF MISSISSIPPI CORPORATION'],
                    'ARKANSAS BLUE CROSS BLUE SHIELD':['BCBS Arkansas','BCBS AR','ARKANSAS BLUE CROSS BLUE SHIELD'],
                    'STATE OF IOWA':['Iowa','STATE OF IOWA'],
                    'SAV-RX':['Sav-rx','Sav','Sav rx','SAV-RX'],
                    'STATE OF WEST VIRGINIA':['WV','West Virginia','STATE OF WEST VIRGINIA'],
                    'CLEAR SPRING HEALTH':['Clear spring','CLEAR SPRING HEALTH'],
                    'KROGER PBM':['Kroger','KROGER PBM'],
                    'HEALTHYDAKOTA MUTUAL HOLDINGS':['Healthydakota','Health dakota','HEALTHYDAKOTA MUTUAL HOLDINGS'],
                    'STATE OF CONNECTICUT':['Connecticut','STATE OF CONNECTICUT'],
                    'STATE OF ALABAMA':['Alabama','STATE OF ALABAMA'],
                    'BLUE SHIELD OF CALIFORNIA':['BS CA','BS California','BLUE SHIELD OF CALIFORNIA'],
                    'CAPITAL BLUECROSS, INC.':['Capital','Capital BC','CAPITAL BLUECROSS, INC.'],
                    'PACIFICSOURCE HEALTH PLAN':['Pacificsource','PACIFICSOURCE HEALTH PLAN'],
                    'BLUE CROSS AND BLUE SHIELD OF KANSAS':['BCBS KA','BCBS Kansas','BLUE CROSS AND BLUE SHIELD OF KANSAS'],
                    'MAGELLAN RX MANAGEMENT':['Magellan','Magellan Rx','MAGELLAN RX MANAGEMENT'],
                    'CARESOURCE MANAGEMENT GROUP':['Caresource',' Care Source','CARESOURCE MANAGEMENT GROUP'],
                    'HEALTHFIRST':['Healthfirst','Health First','HEALTHFIRST'],
                    'UPMC HEALTH SYSTEM':['UPMC','University of Pittsburgh','UPMC HEALTH SYSTEM'],
                    'BLUE CROSS AND BLUE SHIELD OF NEBRASKA':['BCBS NE','BCBS Nebraska','BLUE CROSS AND BLUE SHIELD OF NEBRASKA'],
                    'GOVERNMENT EMPLOYEES HEALTH ASSOCIATION (GEHA)':['GEHA','GOVERNMENT EMPLOYEES HEALTH ASSOCIATION (GEHA)'],
                    'STATE OF MISSISSIPPI':['Mississippi','STATE OF MISSISSIPPI'],
                    'MVP HEALTH CARE, INC.':['Mvp','MVP healthcare','MVP HEALTH CARE, INC.'],
                    'STATE OF IDAHO':['Idaho','STATE OF IDAHO'],
                    'ALLUMA PBM':['Alluma','ALLUMA PBM'],
                    'PREFERREDONE CORPORATION':['Preferredone','Preferred one','PREFERREDONE CORPORATION'],
                    'SOUTHWEST CATHOLIC HEALTH NETWORK CORPORATION':['SW Catholic','Southwest catholic','SOUTHWEST CATHOLIC HEALTH NETWORK CORPORATION'],
                    'STATE OF MAINE':['Maine','STATE OF MAINE'],
                    'BLUE CROSS BLUE SHIELD OF RHODE ISLAND, INC.':['BCBS RI','BCBS Rhode Island','BLUE CROSS BLUE SHIELD OF RHODE ISLAND, INC.'],
                    'CAPITAL DISTRICT PHYSICIANS HEALTH PLAN, INC.':['CD Physicians','Capital district','Capital district physicians','CAPITAL DISTRICT PHYSICIANS HEALTH PLAN, INC.'],
                    'BENECARD SERVICES':['Benecard','BENECARD SERVICES'],
                    'STATE OF GEORGIA':['Georgia','STATE OF GEORGIA'],
                    'STATE OF MONTANA':['Montana','STATE OF MONTANA'],
                    'TRITON HEALTH SYSTEMS':['Triton','Triton Health','TRITON HEALTH SYSTEMS'],
                    'COOK COUNTY HEALTH AND HOSPITAL SYSTEM':['Cook county','COOK COUNTY HEALTH AND HOSPITAL SYSTEM'],
                    'MHBP':['Mhbp','MHBP'],
                    'SANFORD HEALTH PLAN':['Sanford','Sanford Health','SANFORD HEALTH PLAN'],
                    'MC-RX':['MCRX','MC Rx','MC-RX'],
                    'VENTEGRA, LLC':['Ventegra','VENTEGRA, LLC'],
                    'BLUE CROSS BLUE SHIELD OF SOUTH CAROLINA':['BCBS SC','BCBS South Carolina','BLUE CROSS BLUE SHIELD OF SOUTH CAROLINA'],
                    'ELIXIR INSURANCE':['Elixir','ELIXIR INSURANCE'],
                    'EPIPHANYRX':['Epiphanyrx','EPIPHANYRX'],
                    'MARSHFIELD CLINIC':['Marshfield','MARSHFIELD CLINIC'],
                    'BOSTON MEDICAL CENTER HEALTH PLAN, INC.':['BMC','Boston Medical Center','BOSTON MEDICAL CENTER HEALTH PLAN, INC.'],
                    'STATE OF ALASKA':['Alaska','STATE OF ALASKA'],
                    'THE CARLE FOUNDATION':['Carle','Carle Foundation','THE CARLE FOUNDATION'],
                    'COMMUNITY HEALTH NETWORK OF WASHINGTON':['CHN Washington','Community Health Network Washington','COMMUNITY HEALTH NETWORK OF WASHINGTON'],
                    'BAYLOR SCOTT & WHITE HEALTH':['Baylor Scott White','Baylor','Baylor Scott','BAYLOR SCOTT & WHITE HEALTH'],
                    'NEIGHBORHOOD HEALTH PLAN OF RHODE ISLAND':['NHP RI','Neighborhood RI','NEIGHBORHOOD HEALTH PLAN OF RHODE ISLAND'],
                    'EXPRESS SCRIPTS, INC.':['ESI','Express','EXPRESS SCRIPTS, INC.'],
                    'STATE OF UTAH':['Utah','STATE OF UTAH'],
                    'BANNER HEALTH':['Banner','BANNER HEALTH'],
                    'UNIVERSITY OF UTAH HEALTH PLANS':['UofUtah','UoU','UNIVERSITY OF UTAH HEALTH PLANS'],
                    'MAXOR PLUS':['Maxor','MAXOR PLUS'],
                    'AVERA HEALTH':['Avera','AVERA HEALTH'],
                    'SMITHRX PBM':['Smith rx','SMITHRX PBM'],
                    'SCAN HEALTH PLAN, INC.':['Scanhealth','Scan health','SCAN HEALTH PLAN, INC.'],
                    'STATE OF ARKANSAS':['Arkansas','STATE OF ARKANSAS'],
                    'INTERMOUNTAIN HEALTH':['Intermountain','INTERMOUNTAIN HEALTH']}
                    

pbm_dict = {'Optum': ['Optum PBM', 'OptumRx PBM', 'PBM Optum', 'PBM OptumRx'],
            'Express Scripts': ['Express Scripts Inc PBM', 'ESI PBM', 'Express Scripts PBM', 'PBM Express Scripts Inc', 'PBM ESI', 'PBM Express Scripts'],
            'Caremark': ['CVS PBM', 'CVS Caremark PBM', 'Caremark PBM', 'PBM CVS', 'PBM Caremark', 'PBM CVS Caremark'],
            'OPTUMRX':['PBM Optum','Optum PBM','OPTUMRX PBM','PBM OPTUMRX'],
'EXPRESS SCRIPTS':['ESI PBM','PBM ESI','EXPRESS SCRIPTS PBM','PBM EXPRESS SCRIPTS'],
'CAREMARK':['Caremark PBM','PBM Caremark','CAREMARK PBM','PBM CAREMARK'],
'PRIME THERAPEUTICS':['Prime PBM','PBM Prime','PRIME THERAPEUTICS PBM','PBM PRIME THERAPEUTICS'],
'SS&C HEALTH':['SSC PBM','PBM SSC','SS&C HEALTH PBM','PBM SS&C HEALTH'],
'AETNA PHARMACY MGT':['Aetna PBM','PBM Aetna','AETNA PHARMACY MGT PBM','PBM AETNA PHARMACY MGT'],
'MEDIMPACT/MEDCARE':['Medimpact PBM','PBM Medimpact','MEDIMPACT/MEDCARE PBM','PBM MEDIMPACT/MEDCARE'],
'MAGELLAN RX MGT':['Magellan Rx PBM','Magellan PBM','MAGELLAN RX MGT PBM','PBM MAGELLAN RX MGT','PBM Magellan'],
'CARELONRX':['Carelon PBM','PBM Carelon','CARELONRX PBM','PBM CARELONRX'],
'MCKESSON HEALTH SOLS':['Mckesson PBM','PBM Mckesson','MCKESSON HEALTH SOLS PBM','PBM MCKESSON HEALTH SOLS'],
'ELIXIR':['Elixir PBM','PBM Elixir','ELIXIR PBM','PBM ELIXIR'],
'OPUS HLTH INFO SYS':['Opus PBM','PBM Opus','OPUS HLTH INFO SYS PBM','PBM OPUS HLTH INFO SYS','Opus Health PBM','PBM Opus Health'],
'PHARMACY DATA MGT':['Pharmacy Data PBM','PBM Pharmacy Data','PHARMACY DATA MGT PBM','PBM PHARMACY DATA MGT'],
'NAVITUS HLTH SOLUTNS':['Navitus PBM','PBM Navitus','NAVITUS HLTH SOLUTNS PBM','PBM NAVITUS HLTH SOLUTNS'],
'ABARCA HEALTH':['Abarca PBM','PBM Abarca','ABARCA HEALTH PBM','PBM ABARCA HEALTH'],
'HP ENTERPRISE SVCS':['PBM HP','HP PBM','HP ENTERPRISE SVCS PBM','PBM HP ENTERPRISE SVCS'],
'COMPUTER SCIENCES CORP':['PBM COMPUTER SCIENCES','COMPUTER SCIENCES PBM','COMPUTER SCIENCES CORP PBM','PBM COMPUTER SCIENCES CORP'],
'CHANGE HEALTHCARE':['PBM CHANGE','CHANGE PBM','CHANGE HEALTHCARE PBM','PBM CHANGE HEALTHCARE'],
'XEROX':['PBM XEROX','XEROX PBM','XEROX PBM','PBM XEROX'],
'GAINWELL':['PBM GAINWELL','GAINWELL PBM','GAINWELL PBM','PBM GAINWELL'],
'CAPITAL RX':['PBM CAPITAL RX','CAPITAL RX PBM','CAPITAL RX PBM','PBM CAPITAL RX'],
'GOODRX':['PBM GOODRX','GOODRX PBM','GOODRX PBM','PBM GOODRX'],
'RXSENSE':['PBM RX SENSE','RX SENSE PBM','RXSENSE PBM','PBM RXSENSE'],
'SAV-RX':['PBM SAV RX','SAV RX PBM','SAV-RX PBM','PBM SAV-RX'],
'LIVINITI':['PBM LIVINITI','LIVINITI PBM','LIVINITI PBM','PBM LIVINITI'],
'BCBSMS SELF PROC':['PBM BCBS MS','BCBS MS PBM','BCBSMS SELF PROC PBM','PBM BCBSMS SELF PROC','Bluecross Blueshield Mississippi PBM','BCBS Mississippi PBM'],
'UNISYS PROCESSOR':['PBM UNISYS','UNISYS PBM','UNISYS PROCESSOR PBM','PBM UNISYS PROCESSOR'],
'ALLWIN DATA SVCS/ERX':['PBM ALLWIN','ALLWIN PBM','ALLWIN DATA SVCS/ERX PBM','PBM ALLWIN DATA SVCS/ERX'],
'GHS DATA MGT/MEDE SLT':['PBM GHS','GHS PBM','GHS DATA MGT/MEDE SLT PBM','PBM GHS DATA MGT/MEDE SLT'],
'BENECARD SERVICES':['PBM BENECARD','BENECARD PBM','BENECARD SERVICES PBM','PBM BENECARD SERVICES'],
'SCRIPT CARE':['PBM SCRIPT CARE','SCRIPT CARE PBM','SCRIPT CARE PBM','PBM SCRIPT CARE'],
'OMNISYS/CARECLAIM':['PBM OMNISYS','OMNISYS PBM','OMNISYS/CARECLAIM PBM','PBM OMNISYS/CARECLAIM','PBM CARECLAIM','CARECLAIM PBM'],
'PROCARE RX':['PBM PROCARE','PROCARE PBM','PROCARE RX PBM','PBM PROCARE RX'],
'VENTEGRA':['PBM VENTEGRA','VENTEGRA PBM','VENTEGRA PBM','PBM VENTEGRA'],
'PHARMPIX':['PBM PHARMPIX','PHARMPIX PBM','PHARMPIX PBM','PBM PHARMPIX'],
'PARAMOUNT RX':['PBM PARAMOUNT','PARAMOUNT PBM','PARAMOUNT RX PBM','PBM PARAMOUNT RX'],
'COSTCO HLTH SOLUTNS':['PBM COSTCO','COSTCO PBM','COSTCO HLTH SOLUTNS PBM','PBM COSTCO HLTH SOLUTNS'],
'MC-21 CORPORATION':['PBM MC21','MC21 PBM','MC-21 CORPORATION PBM','PBM MC-21 CORPORATION'],
'MAXORPLUS':['PBM MAXORPLUS','MAXORPLUS PBM','MAXORPLUS PBM','PBM MAXORPLUS'],
'WELLDYNERX/RXWEST':['PBM WELLDYNE','WELLDYNE PBM','WELLDYNERX/RXWEST PBM','PBM WELLDYNERX/RXWEST','PBM RXWEST','RXWEST PBM'],
'RXADVANCE UNSPEC':['PBM RXADVANCE','RXADVANCE PBM','RXADVANCE UNSPEC PBM','PBM RXADVANCE UNSPEC'],
'REALRX':['PBM REALRX','REALRX PBM','REALRX PBM','PBM REALRX'],
'SMITHRX':['PBM SMITHRX','SMITHRX PBM','SMITHRX PBM','PBM SMITHRX'],
'SLCT HLTH SELF PROC':['PBM SLCT','SLCT PBM','SLCT HLTH SELF PROC PBM','PBM SLCT HLTH SELF PROC'],
'CLEARSCRIPT UNSPEC':['PBM CLEARSCRIPT','CLEARSCRIPT PBM','CLEARSCRIPT UNSPEC PBM','PBM CLEARSCRIPT UNSPEC'],
'MEDONE':['PBM MEDONE','MEDONE PBM','MEDONE PBM','PBM MEDONE'],
'EMP HLTH IN MGT/EHIM':['PBM EMP Health','EMP Health PBM','EMP HLTH IN MGT/EHIM PBM','PBM EMP HLTH IN MGT/EHIM'],
'EPIPHANYRX':['PBM EPIPHANYRX','EPIPHANYRX PBM','EPIPHANYRX PBM','PBM EPIPHANYRX'],
'IHA SELF PROC':['PBM IHA','IHA PBM','IHA SELF PROC PBM','PBM IHA SELF PROC'],
'PHARMAVAIL BNFT MGMT':['PBM PHARMAVAIL','PHARMAVAIL PBM','PHARMAVAIL BNFT MGMT PBM','PBM PHARMAVAIL BNFT MGMT'],
'CERPASSRX':['PBM CERPASSRX','CERPASSRX PBM','CERPASSRX PBM','PBM CERPASSRX'],
'MERIDIANRX':['PBM MERIDIANRX','MERIDIANRX PBM','MERIDIANRX PBM','PBM MERIDIANRX','PBM MERIDIAN','MERIDIAN PBM'],
'SCRIPTCLAIM SYSTEMS':['PBM SCRIPTCLAIM','SCRIPTCLAIM PBM','SCRIPTCLAIM SYSTEMS PBM','PBM SCRIPTCLAIM SYSTEMS'],
'EMPLOYER HLTH OPTION':['PBM EMPLOYER HEALTH','EMPLOYER HEALTH PBM','EMPLOYER HLTH OPTION PBM','PBM EMPLOYER HLTH OPTION'],
'HEALTHSMART RX':['PBM HEALTHSMART','HEALTHSMART PBM','HEALTHSMART RX PBM','PBM HEALTHSMART RX'],
'GENERAL PRESC PROG':['PBM GENERAL PRESCRIPTION PROGRAM','GENERAL PRESCRIPTION PROGRAM PBM','GENERAL PRESC PROG PBM','PBM GENERAL PRESC PROG'],
'MAXCARE RX':['PBM MAXCARE','MAXCARE PBM','MAXCARE RX PBM','PBM MAXCARE RX'],
'ENVOLVE PHARM SOL':['PBM ENVOLVE','ENVOLVE PBM','ENVOLVE PHARM SOL PBM','PBM ENVOLVE PHARM SOL'],
'PHARMASTAR':['PBM PHARMASTAR','PHARMASTAR PBM','PHARMASTAR PBM','PBM PHARMASTAR'],
'PERFORM RX':['PBM PERFORMRX','PERFORMRX PBM','PERFORM RX PBM','PBM PERFORM RX'],
'PINNACLE PRES MGMT':['PBM PINNACLE','PINNACLE PBM','PINNACLE PRES MGMT PBM','PBM PINNACLE PRES MGMT'],
'AMWINS RX':['PBM AMWINS','AMWINS PBM','AMWINS RX PBM','PBM AMWINS RX'],
'CITIZENS RX LLC':['PBM CITIZENSRX','CITIZENSRX PBM','CITIZENS RX LLC PBM','PBM CITIZENS RX LLC'],
'SERVE YOU PRESC MGMT':['PBM SERVEYOU','SERVEYOU PBM','SERVE YOU PRESC MGMT PBM','PBM SERVE YOU PRESC MGMT'],
'IPM/INTG PRES MGT':['PBM IPM','IPM PBM','IPM/INTG PRES MGT PBM','PBM IPM/INTG PRES MGT','PBM INTG','INTG PBM'],
'DREXI':['PBM DREXI','DREXI PBM','DREXI PBM','PBM DREXI'],
'AVIA PARTNERS':['PBM AVIA','AVIA PBM','AVIA PARTNERS PBM','PBM AVIA PARTNERS'],
'PRESCRYPTIVE HEALTH':['PBM PRESCRYPTIVE','PRESCRYPTIVE PBM','PRESCRYPTIVE HEALTH PBM','PBM PRESCRYPTIVE HEALTH'],
'SCRIPTGUIDE RX':['PBM SCRIPTGUIDE','SCRIPTGUIDE PBM','SCRIPTGUIDE RX PBM','PBM SCRIPTGUIDE RX'],
'AMERICAN HLTHCARE':['PBM AMERICAN','AMERICAN PBM','AMERICAN HLTHCARE PBM','PBM AMERICAN HLTHCARE'],
'MONARCH SPECIALTY':['PBM MONARCH','MONARCH PBM','MONARCH SPECIALTY PBM','PBM MONARCH SPECIALTY'],
'SUN RX':['PBM SUNRX','SUNRX PBM','SUN RX PBM','PBM SUN RX'],
'HCC EZ-DME':['PBM HCC','HCC PBM','HCC EZ-DME PBM','PBM HCC EZ-DME'],
'RXEDO':['PBM RXEDO','RXEDO PBM','RXEDO PBM','PBM RXEDO'],
'SCRIPTCYCLE LLC':['PBM SCRIPTCYCLE','SCRIPTCYCLE PBM','SCRIPTCYCLE LLC PBM','PBM SCRIPTCYCLE LLC'],
'PHOENIX BEN MGMT':['PBM PHOENIX','PHOENIX PBM','PHOENIX BEN MGMT PBM','PBM PHOENIX BEN MGMT'],
'WITH ME HEALTH':['PBM WITHME','WITHME PBM','WITH ME HEALTH PBM','PBM WITH ME HEALTH'],
'PMSI':['PBM PMSI','PMSI PBM','PMSI PBM','PBM PMSI'],
'TRUST PLUS':['PBM TRUSTPLUS','TRUSTPLUS PBM','TRUST PLUS PBM','PBM TRUST PLUS'],
'VRX':['PBM VRX','VRX PBM','VRX PBM','PBM VRX'],
'HEALTHESYSTEMS':['PBM HEALTHESYSTEMS','HEALTHESYSTEMS PBM','HEALTHESYSTEMS PBM','PBM HEALTHESYSTEMS'],
'TRUE CARE PHARMACY':['PBM TRUECARE','TRUECARE PBM','TRUE CARE PHARMACY PBM','PBM TRUE CARE PHARMACY'],
'ACLAIM INC':['PBM ACLAIM','ACLAIM PBM','ACLAIM INC PBM','PBM ACLAIM INC'],
'UNIVERSAL RX':['PBM UNIVERSALRX','UNIVERSALRX PBM','UNIVERSAL RX PBM','PBM UNIVERSAL RX'],
'OMEDA RX':['PBM OMEDA','OMEDA PBM','OMEDA RX PBM','PBM OMEDA RX'],
'SMITH DATA PROCESSNG':['PBM SMITH DATA','SMITH DATA PBM','SMITH DATA PROCESSNG PBM','PBM SMITH DATA PROCESSNG'],
'CATALYST RX':['PBM CATALYST RX','CATALYST RX PBM','CATALYST RX PBM','PBM CATALYST RX'],
'MEDCO HLTH SOLUTIONS':['PBM MEDCO HEALTH','MEDCO HEALTH PBM','MEDCO HLTH SOLUTIONS PBM','PBM MEDCO HLTH SOLUTIONS','MEDCO PBM','PBM MEDCO'],
'NATL MEDICAL HLT CARD':['PBM NATIONAL MEDICAL','NATIONAL MEDICAL PBM','NATL MEDICAL HLT CARD PBM','PBM NATL MEDICAL HLT CARD'],
'BEYOND RX':['PBM BEYOND RX','BEYOND RX PBM','BEYOND RX PBM','PBM BEYOND RX'],
'ADVANCEPCS':['PBM ADVANCE PCS','ADVANCE PCS PBM','ADVANCEPCS PBM','PBM ADVANCEPCS'],
'PBM PLUS':['PBM PBMPLUS','PBMPLUS PBM','PBM PLUS PBM','PBM PBM PLUS'],
'LDI PHARM BENFT MGMT':['PBM LDI PHARM','LDI PHARM PBM','LDI PHARM BENFT MGMT PBM','PBM LDI PHARM BENFT MGMT'],
'FUTURESCRIPTS':['PBM FUTURE SCRIPTS','FUTURE SCRIPTS PBM','FUTURESCRIPTS PBM','PBM FUTURESCRIPTS'],
'NDC':['PBM NDC','NDC PBM','NDC PBM','PBM NDC'],
'PROACT PHARM SERV':['PBM PROACT','PROACT PBM','PROACT PHARM SERV PBM','PBM PROACT PHARM SERV'],
'INTEGRATED PRES SOLS':['PBM INTEGRATED PRESCRIPTIONS','INTEGRATED PRESCRIPTIONS PBM','INTEGRATED PRES SOLS PBM','PBM INTEGRATED PRES SOLS'],
'RXAMERICA':['PBM RXAMERICA','RXAMERICA PBM','RXAMERICA PBM','PBM RXAMERICA'],
'MEDTRAK SERVICES':['PBM MEDTRAK','MEDTRAK PBM','MEDTRAK SERVICES PBM','PBM MEDTRAK SERVICES'],
'WALGREENS HLTH INIT':['PBM WALGREENS','WALGREENS PBM','WALGREENS HLTH INIT PBM','PBM WALGREENS HLTH INIT']}

rmo_dict = {'Optum Govt': ['Optum Government', 'Optum Govt', 'Optum Gov'],
            'Optum Medicare': ['Optum Medicare', 'Optum RMO Medicare'],
            'CVS Govt': ['CVS Government', 'CVS Govt', 'CVS Gov'],
            'Optum Comm Non-EMISAR': ['Optum Comm Non-EMISAR', 'Optum Comm not EMISAR'],
            'Ascent': ['Ascent'],
            'CVS Medicare': ['CVS Medicare'],
            'ESI Medicare': ['ESI Medicare'],
            'Zinc Health': ['Zinc', 'Zinc Health'],
            'ESI Govt': ['ESI Government', 'ESI Govt', 'ESI Gov']
        }
benefittype_dict = {'Rx': ['Rx', 'Retail','Pharmacy benefit', 'Pharmacy'],
                    'Mx': ['Mx', 'Mx benefit', 'Medical benefit', 'Buy and Bill']}

market_dict = {'inflamm': ['Inflammation', 'Inflam', 'Inflamm'],
               'resp': ['Severe asthma', 'asthma', 'SA','respiratory','resp'],
               'bone': ['Bone','bone','Osteoporosis','pmo','PMO'],
               'tavneos': ['tavn','tavneos','tavneoss'],
               'wac': ['wac','pricing','wholesale']
              }

formulary_dict = {'ESI National': ['esi national formulary', 'ESI NPF'],
                  'ESI Custom': ['esi custom'],
                  'Prime': ['Prime', 'Prime Therapeutics'],
                  'Cigna': ['Cigna'],
                  'CVS NTF': ['CVS NTF', 'CVS National Formulary', 'CVS National'],
                  'Anthem':['IngenioRx', 'Ingenio', 'Anthem'],
                  'Optum National': ['Optum National', 'Optum National Formulary'],
                  'Optum Custom': ['Optum Custom']
                 }

                 
timeperiod_mapping = {'Rx claims': ['fill_date'],
                      'Mx claims': ['service_date']}
    
operator_dict = {'average':['average', 'mean'],
                 'sum': ['sum', 'sum of', 'total'],
                 'count': ['count', 'distinct', 'number of', 'how many'],
                 'median': ['median']
                }
visualization_dict = {'Histogram': ['histogram', 'buckets', 'distribution'],
                      'line chart': ['trend', 'trend line'],
                      'pie chart': ['pie'],
                      'table': ['table', 'tabular', 'data dump']
                     }

splitby_dict = {'timeperiod_year': ['by year', 'yearly'],
                'timeperiod_month': ['by month', 'monthly'],
                'timeperiod_quarter': ['by quarter', 'by qtr', 'quarterly', 'quarter', 'qtr' ],
                # 'timeperiod_week': ['by week', 'weekly'],
                'indication': ['by indication', 'indication wise','indication'],
                'brand':['by drug','by brand','by brands', 'brand wise','brand'],
                'ndc': ['by ndc','by ndc_id', 'by ndc code','ndc'],
                'channel': ['by channel', 'by payment', 'channel wise','channel'],
                'controller': ['by controller', 'controller wise', 'controller'],
                'pbm': ['by pbm', 'pbm wise','pbm'],
                'formulary': ['by formulary', 'formulary wise'],
                'oop_group': ['oop bucket', 'oop buckets', 'patient oop bucket', 'patient out of pocket bucket'],
                'specialty': ['byspecialty', 'by PCP', 'by SP', 'PCP vs SP', 'PCP SP'],
                'reject_reason':['by reason','by rejection reason'],
                'plan_name':['by plan','by plan name'],
                'claim_type':['by status','by claim status','by claim type'],
                'bsa': ['bsa', 'bsa requirement', 'body surface area'],
                'territory': ['terr, territory'],
                'district': ['dist', 'district'],
                'region': ['region'],
                'spreq': ['specialist requirement', 'specialist', 'specialist required'],
                'documentation': ['document diagnosis', 'documentation requirement', 'documentation required'],
                'um': ['um criteria', 'um requirements', 'um details', 'utilization management criteria', 'utilization management requirements'],
                'steps': ['steps', 'step requirement', 'step through', 'step-through'],
                'step_statement': ['step statement', 'step through products'],
                'medicare_patienttype': ['std vs lis', 'std vs. lis']
                }

splitby = {'timeperiod':'',
            'channel':'',
            'indication':'',
            'controller':'',
            'pbm':'',
            'brand':'',
            'oop':'',
            'specialty':'',
            'state':'',
            'territory':'',
            'district':'',
            'region':''
           }



# COMMAND ----------

#!pip install nltk
#!pip install fuzzywuzzy
#!pip install dateparser

#!pip install spacy dateparser python-dateutil
#!python -m spacy download en_core_web_sm 


#!pip install nltk
#!pip install fuzzywuzzy

from dateutil.parser import parse
import copy
import dateutil.parser as parser
from fuzzywuzzy import fuzz
import nltk
nltk.download('stopwords')
#python -m spacy download en_core_web_sm

import re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
#!pip install dateparser
import dateparser
import spacy
from typing import List, Tuple, Dict, Optional

# Initialize spaCy English model
nlp = spacy.load("en_core_web_sm")


nltk.download('punkt_tab')
from nltk.tokenize import sent_tokenize, word_tokenize 
from nltk.corpus import stopwords
from datetime import datetime

# IMPORTS
import warnings



# HELPER FUNCTIONS
def custom_formatwarning(msg, *args, **kwargs):
    # ignore everything except the message
    return str(msg) + '\n'
warnings.formatwarning = custom_formatwarning

import re

# --------------------- Helper Functions ---------------------

# Helper function to convert list to SQL string
def split_list_to_string(input_list):
    return "('" + "|".join([f"{x}" for x in input_list]) + "')"

# Helper function to convert list to SQL string
def split_list_to_brackets(input_list):
    return "(" + ",".join([f"'{x}'" for x in input_list]) + ")"


# Helper function to prefix column names with an alias in filter conditions
def prefix_columns(filter_condition, sql_colnames, alias='a.'):
    """
    Prefix column names in the filter_condition with the given alias.
    Only exact matches of column names are prefixed.
    """
    # Sort column names by length descending to avoid partial matches
    sorted_columns = sorted(sql_colnames.values(), key=lambda x: -len(x))
    for col in sorted_columns:
        # Use regex to replace exact word matches
        pattern = r'\b{}\b'.format(re.escape(col))
        replacement = f"{alias}{col}"
        filter_condition = re.sub(pattern, replacement, filter_condition)
    return filter_condition

# Helper function to remove duplicates while preserving order
def dedupe(lst):
    seen = set()
    return [x for x in lst if not (x in seen or seen.add(x))]

def month_string_to_number(string):
    m = {
        'jan': '01', 'feb': '02','mar': '03','apr':'04','may':'05','jun':'06','jul':'07','aug':'08','sep':'09','oct':'10','nov':'11','dec':'12'
        }
    s = string.strip()[:3].lower()
    try:
        out = m[s]
        return out
    except:
        raise ValueError('Not a month')

def fn_right(s, amount):
    return s[-amount:]

def fn_mid(s, offset, amount):
    return s[offset:offset+amount]

def is_year(s):
    return s.isdigit() and 1900 <= int(s) <= 2100
def fn_entity_score(entity_name, entity_dict, to_check, overall, thresholds):
    for key, lst_items in entity_dict.items():
        for item in lst_items:
            ratio = fuzz.ratio(to_check.lower(), item.lower())
            if overall['max'] < ratio and ratio > thresholds.get(entity_name,80) and to_check not in commonwords_dict:
              overall.update({'max': ratio, 'lst': to_check, 'name': key, 'entity': entity_name})
    return {
        "overall": overall.get('lst', ''),
        "overall_name": overall.get('name', ''),
        "overall_entity": overall.get('entity', ''),
        "overall_maxscore": overall.get('max', 0)
    }

def fn_entity_individual_score(entity_name, entity_dict, to_check, overall, thresholds):
    counter = 0
    for key, lst_items in entity_dict.items():
        for item in lst_items:
            for item1 in item.split():
                ratio = fuzz.ratio(to_check.lower(), item1.lower())
                #print(to_check.lower(), item1.lower(), ratio)
                if ratio > thresholds.get(entity_name,80):
                    counter = 1
                    
    return counter


def fn_scoring(metrics_dict, brand_dict, custom_dict, timeperiod_dict, channel_dict,
               indication_dict, controller_dict, pbm_dict, rmo_dict, benefittype_dict, formulary_dict, market_dict, market_products_dict, splitby_dict,grouping_dict,datasource_dict,thresholds,primarysearch,user_input):



    final_selection = {
        'metrics': [],
        'market_name': [],
        'start_date': '',
        'end_date': '',
        'product_names': [],
        'channel_names': [],
        'controller_names': [],
        'pbm_names': [],
        'rmo_names': [],
        'benefittype_names': [],
        'indication_names': [],
        'metric_frequency': '',
        'grouping_names': [],
        'data_source_names': [],
        'cut_by': [],
        'excluded_metrics': [],
        'excluded_market_name': '',
        'excluded_product_names': [],
        'excluded_channel_names': [],
        'excluded_controller_names': [],
        'excluded_pbm_names': [],
        'excluded_rmo_names': [],
        'excluded_benefittype_names': [],
        'excluded_indication_names': [],
        'excluded_grouping_names': [],
        'excluded_data_source_names': [],
        'partition_cut_by': [],
        'timeperiod': {
            'from_period': None,
            'from_unit': None,
            'to_period': None,
            'to_unit': None,
            'relative_unit': None,
            'relative_number': None,
            'excluded_periods': []
        }
    }
    #NEW ADDITION FOR EXCLUSION
    exclusion_pattern = r"(?:excluding|without|not having|exclude)\s+(.+)"
    exclusion_matches_raw = re.findall(exclusion_pattern, user_input, re.IGNORECASE)
    print("EM RAW :", exclusion_matches_raw)
    exclusion_matches = exclusion_matches_raw[0].split() if exclusion_matches_raw else []
    exclusion_matches1 = [match.lower() for match in exclusion_matches]
    print("\nExclusion Matches:", exclusion_matches)
    print("\nExclusion Matches1:", exclusion_matches1)

    print("\n\n\nPrimary Search ",primarysearch)

    top_pattern_with_word = r'top (\d+)\s+([\w\s]+?)(?=\s+by|\s*$)'
    top_matches_with_word = re.findall(top_pattern_with_word, user_input, re.IGNORECASE)
    # Extract just the word after "top n" and check if it's in cut_by
    words_after_top = [match[1].strip() for match in top_matches_with_word]
    #cut_by_for_partition = [word for word in cut_by if word not in words_after_top]
    print("\nTop Matches with Words in final select:", top_matches_with_word)
    print("\nWords After Top in final select :", words_after_top)

    #print("\nCut By For Partition in final select :", cut_by_for_partition)
    top_pattern = r'top (\d+)'
    top_matches_raw = re.findall(top_pattern, user_input, re.IGNORECASE)
    top_matches = top_matches_raw[0].split() if top_matches_raw else []
    top_matches1 = [match.lower() for match in top_matches]
    #print("\nTop Matches:", top_matches)
    #print("\nTop Matches1:", top_matches1)
    if top_matches:
        for top in top_matches:
            user_input = user_input.replace(f"top {top}", "")
    
    word_before_average_pattern = r"(\w+)\s+(?:average|avg)"
    word_before_average_matches = re.findall(word_before_average_pattern, user_input, re.IGNORECASE)
    if word_before_average_matches:
        for words in word_before_average_matches:
            user_input = user_input.replace(f"{words} average", "")
            user_input = user_input.replace(f"{words} avg", "")

   
    #print("USER INPUT R Check : ", user_input)

    updated_primary_search = user_input.split()
    #print("Updated Primary Search : ", updated_primary_search)

    stopWords = set(stopwords.words('english')).union({"what", "why", ",", ":", ".", "and"})
    copy = [w for w in updated_primary_search if w.lower() not in stopWords and w.lower() not in exclusion_matches1]
    copy_1 = [w for w in updated_primary_search if w.lower() not in stopWords]
    # copy = primarysearch
    # print(primarysearch, copy)
    print("\nPrimary Search in my function : ",updated_primary_search)
    print("\nCopy Check : ", copy)

    def process_segments(copy, entity_dicts, fuzzymatch_thresholds):
        final_scoring_dict = {}
        text_to_check = ''
        count = 0
        tmp_count = 0
        tmp = 0
        prev_score = {"overall": '', "overall_name": '', "overall_entity": '', "overall_maxscore": '0'}
        while len(copy) > 0:
                # print(count)
                text_to_check = copy[count]
                # Score against all entities
                overall = {'max':0, 'lst':'', 'name':'', 'entity':''}
                counter = 0
                for entity_name, entity_dict in entity_dicts:
                    if fn_entity_individual_score(entity_name, entity_dict, text_to_check, overall, entity_fuzzymatch_threshold_lst) == 1:
                        counter = 1
                        # print(text_to_check, counter)
                if counter == 1:
                    count = count + 1
                else:
                    # print(111, copy)
                    del(copy[count])
                    # print(222, copy)
                if count >= len(copy):
                    break
        # print(copy)
        count = 0
        text_to_check = ""
        while len(copy) > 0:
            if text_to_check == "":
                text_to_check = copy[count]
            elif count < len(copy):
                text_to_check = text_to_check + " " + copy[count]
            overall = {'max': 0, 'lst': '', 'name': '', 'entity': ''}
        # Score against all entities
            for entity_name, entity_dict in entity_dicts:
                print("Checking Text to check for debug : ",text_to_check)
                score = fn_entity_score(entity_name, entity_dict, text_to_check, overall, entity_fuzzymatch_threshold_lst)
            print(text_to_check, score, prev_score)
            if int(score['overall_maxscore']) == 0 and count == 0 and count <= len(copy):
                # No match for the first word, move to next word
                count += 1
            elif int(score['overall_maxscore']) == 0 and int(prev_score['overall_maxscore']) == 0 and count > 0 and count < len(copy):
            # No match for the current phrase, move to next word
                count += 1
            elif int(prev_score['overall_maxscore']) <= int(score['overall_maxscore']) and int(score['overall_maxscore']) > 0 and count < len(copy):
            # Update the best score and continue
                prev_score = score
                count += 1
            elif int(prev_score['overall_maxscore']) > 0 or (int(score['overall_maxscore']) > 0 and count == len(copy) - 1):
            # Match found; finalize the phrase
                for i in range(0, count):
                    del copy[0]
                final_scoring_dict[tmp_count] = prev_score
                tmp_count += 1
                text_to_check = ''
                count = 0
                prev_score = {"overall": '', "overall_name": '', "overall_entity": '', "overall_maxscore": 0}
            elif count == len(copy):
                # Fallback logic: Start removing the first word and check remaining phrase
                temp_phrases = text_to_check.split()
                print(f"Starting fallback with text_to_check: '{text_to_check}', temp_phrases: {temp_phrases}")
                fallback_found = False  # Track if a valid fallback match is found
                for i in range(1, len(temp_phrases)):  # Skip the first `i` words
                    for j in range(i + 1, len(temp_phrases) + 1):  # Incrementally add words from index `i`
                        fallback_text = " ".join(temp_phrases[i:j])  # Generate a substring
                        print(f"Checking fallback_text: '{fallback_text}'")
                        fallback_overall = {'max': 0, 'lst': '', 'name': '', 'entity': ''}

                        # Check the fallback text against all entities
                        for entity_name, entity_dict in entity_dicts:
                            print(f"Entity Name: {entity_name}")
                            fallback_score = fn_entity_score(
                                entity_name, entity_dict, fallback_text, fallback_overall, entity_fuzzymatch_threshold_lst
                            )
                            print(f"Entity: {entity_name}, Fallback Score: {fallback_score}")

                            # If a valid match is found, stop further checking
                            if int(fallback_score['overall_maxscore']) > 0:
                                prev_score = fallback_score
                                fallback_found = True
                                print(f"Match found during fallback: {prev_score}")
                                break  # Exit entity loop
                        if fallback_found:
                            break  # Exit word-end loop
                    if fallback_found:
                        break  # Exit word-start loop

                if fallback_found:
                    # Remove matched words from the original copy
                    print("Temp phrases len: ", len(temp_phrases))
                    print("Fallback : ",len(fallback_text.split()))
                    words_to_remove = len(temp_phrases) - len(fallback_text.split())
                    print("Words to remove length : ", words_to_remove)

                    print(f"Removing processed words: {temp_phrases[:words_to_remove]}")
                    del copy[:words_to_remove]
                    final_scoring_dict[tmp_count] = prev_score
                    tmp_count += 1
                    break
                else:
                    # No match found, remove the first word and retry
                    print("No fallback match found. Removing the first word.")
                    del copy[:1]

                # Reset variables for the next iteration
                text_to_check = ''
                count = 0
                prev_score = {"overall": '', "overall_name": '', "overall_entity": '', "overall_maxscore": 0}
        return final_scoring_dict

    entity_dicts_common = [
                ('metrics', metrics_dict),
                ('channel_names', channel_dict),
                ('product_names', brand_dict),
                ('controller_names', controller_dict),
                ('grouping_names', grouping_dict),
                ('pbm_names', pbm_dict),
                ('indication_names', indication_dict),
                ('rmo_names', rmo_dict),
                ('benefittype_names', benefittype_dict),
                ('cut_by', splitby_dict),  # Use splitby_dict here
                ('market_name', market_dict),
                ('data_source_names', datasource_dict)   ]
    
    excluded_entity_dicts_common = [
                ('excluded_metrics', metrics_dict),
                ('excluded_channel_names', channel_dict),
                ('excluded_product_names', brand_dict),
                ('excluded_controller_names', controller_dict),
                ('excluded_grouping_names', grouping_dict),
                ('excluded_pbm_names', pbm_dict),
                ('excluded_indication_names', indication_dict),
                ('excluded_rmo_names', rmo_dict),
                ('excluded_benefittype_names', benefittype_dict),
                ('excluded_cut_by', splitby_dict),  # Use splitby_dict here
                ('excluded_market_name', market_dict),
                ('excluded_data_source_names', datasource_dict)]
    
    partition_dict = [('partition_cut_by', splitby_dict)]


     
    final_scoring_dict = process_segments(copy, entity_dicts_common, entity_fuzzymatch_threshold_lst)
    final_scoring_dict1 = process_segments(exclusion_matches, excluded_entity_dicts_common, entity_fuzzymatch_threshold_lst)
    final_scoring_dict2 = process_segments(words_after_top,partition_dict, entity_fuzzymatch_threshold_lst)

    # print(final_scoring_dict)
    entity_to_key = {
        'metrics': 'metrics',
        'product_names': 'product_names',
        'indication_names': 'indication_names',
        'controller_names': 'controller_names',
        'grouping_names': 'grouping_names',
        'channel_names': 'channel_names',
        'pbm_names': 'pbm_names',
        'rmo_names': 'rmo_names',
        'data_source_names': 'data_source_names',
        'cut_by': 'cut_by',
        'benefittype_names': 'benefittype_names',
        'market_name': 'market_name',
        'excluded_metrics': 'excluded_metrics',
        'excluded_product_names': 'excluded_product_names',
        'excluded_indication_names': 'excluded_indication_names',
        'excluded_controller_names': 'excluded_controller_names',
        'excluded_channel_names': 'excluded_channel_names',
        'excluded_grouping_names': 'excluded_grouping_names',
        'excluded_pbm_names': 'excluded_pbm_names',
        'excluded_rmo_names': 'excluded_rmo_names',
        'excluded_data_source_names': 'excluded_data_source_names',
        'excluded_cut_by': 'excluded_cut_by',
        'excluded_benefittype_names': 'excluded_benefittype_names',
        'excluded_market_name': 'excluded_market_name',
        'partition_cut_by': 'partition_cut_by',
    }

    # Process final_scoring_dict
    for i in range(len(final_scoring_dict)):
        entity = final_scoring_dict[i]['overall_entity']
        if entity in entity_to_key:
            key = entity_to_key[entity]
            final_selection[key].append(final_scoring_dict[i]['overall_name'])
            user_input = user_input.replace(final_scoring_dict[i]['overall'], entity)

    # Process final_scoring_dict1
    for i in range(len(final_scoring_dict1)):
        entity = final_scoring_dict1[i]['overall_entity']
        if entity in entity_to_key:
            key = entity_to_key[entity]
            final_selection[key].append(final_scoring_dict1[i]['overall_name'])
            user_input = user_input.replace(final_scoring_dict1[i]['overall'], entity)

    # Process final_scoring_dict2
    for i in range(len(final_scoring_dict2)):
        entity = final_scoring_dict2[i]['overall_entity']
        if entity in entity_to_key:
            key = entity_to_key[entity]
            final_selection[key].append(final_scoring_dict2[i]['overall_name'])
            user_input = user_input.replace(final_scoring_dict2[i]['overall'], entity)

    print("\nfinal selection I'm checking : ")
    print(final_selection)
    return final_selection# Output final selection and return



def map_timeperiod_to_dates(timeperiod_key):
    today = datetime.today()
    start_date, end_date = '', ''
    if timeperiod_key == 'YTD':
        start_date = today.replace(month=1, day=1).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')
    elif timeperiod_key.startswith('R'):
        num = int(''.join(filter(str.isdigit, timeperiod_key)))
        unit = ''.join(filter(str.isalpha, timeperiod_key))
        if unit == 'M':
            start_date = (today - relativedelta(months=num)).strftime('%Y-%m-%d')
        elif unit == 'W':
            start_date = (today - relativedelta(weeks=num)).strftime('%Y-%m-%d')
        elif unit == 'Y':
            start_date = (today - relativedelta(years=num)).strftime('%Y-%m-%d')
        else:
            start_date = ''
        end_date = today.strftime('%Y-%m-%d')
    else:
        try:
            year = int(timeperiod_key)
            start_date = f"{year}-01-01"
            end_date = f"{year}-12-31"
        except:
            start_date, end_date = '', ''
    return start_date, end_date

def infer_market_name(product_names, market_products_dict, market_found):
    if market_found:
        return market_found
    markets_found = set()
    for product in product_names:
        product_lower = product.lower()
        for market, products in market_products_dict.items():
            if product_lower in [p.lower() for p in products]:
                markets_found.add(market)
    if len(markets_found) == 1:
        return markets_found.pop()
    elif len(markets_found) > 1:
        return 'multiple'
    else:
        return ''

def fn_final_selection(timeperiodfrom, timeperiodto, user_input, primarysearch,market_dict, market_products_dict,grouping_dict,datasource_dict):

    #print(final_scoring_dict)
    final_selection = fn_scoring(metrics_dict, brand_dict, custom_dict, timeperiod_dict, channel_dict,
                                 indication_dict, controller_dict, pbm_dict, rmo_dict, benefittype_dict, formulary_dict, market_dict,market_products_dict, splitby_dict,grouping_dict,datasource_dict,entity_fuzzymatch_threshold_lst,primarysearch,user_input)

    
    frequency_mapping = {
        'quarter': 'q',
        'quarterly': 'q',
        'qtr': 'q',
        'monthly': 'm',
        'month': 'm',
        'yearly': 'y',
        'year': 'y',
        'annual': 'y',
        'annually':'y'
    }
    #print(final_scoring_dict.get(best_match))
    # Determine metric_frequency from user_input
    
    # print(final_scoring_dict.get(best_match))
    # Populate entities
    market_found = None
    timeperiod_key = ''

    # print(final_selection)
    # for p_info in final_scoring_dict:
    #     entity = p_info['overall_entity']
    #     if p_info['overall_maxscore'] > entity_fuzzymatch_threshold:
    #         if entity in final_selection and isinstance(final_selection[entity], list):
    #             if entity == 'market_name' and not final_selection['market_name']:
    #                 final_selection['market_name'] = p_info['overall_name']
    #                 market_found = p_info['overall_name']
    #             elif entity == 'timeperiod':
    #                 # Capture the timeperiod key from the matched entity
    #                 timeperiod_key = p_info['overall_name']
    #             else:
    #                 final_selection[entity].append(p_info['overall_name'])
            
    #         elif entity == 'cut_by':
    #             final_selection['cut_by'].append(p_info['overall_name'])
            # print(entity)

    # # Populate product_names from 'product_names' entity
    # product_entities = final_scoring_dict.get(best_match, {}).get('product_names', {})
    # for _, p_info in product_entities.items():
    #     if p_info['overall_maxscore'] > entity_fuzzymatch_threshold:
    #         final_selection['product_names'].append(p_info['overall_name'])
    metric_frequency = ''
    for item in final_selection['cut_by']:
        if item == 'timeperiod_year':
            metric_frequency = 'y'
        elif item == 'timeperiod_quarter':
            metric_frequency = 'q'
        elif item == 'timeperiod_month':
            metric_frequency = 'm'
    final_selection['metric_frequency'] = metric_frequency

    # Infer market_name based on product_names using market_products_dict if not already found
    inferred_market = infer_market_name(final_selection['product_names'], market_products_dict, market_found)
    if inferred_market:
        final_selection['market_name'] = inferred_market
    import re
    from typing import Tuple, Optional, Dict, Union
    from datetime import datetime, timedelta
    from dateutil.parser import parse
    from dateutil.relativedelta import relativedelta

    def extract_time_periods(text: str, reference_date: datetime = datetime.today()) -> Dict[str, Union[Optional[str], Optional[int]]]:
        """
        Extracts the 'from' and 'to' time periods from user input text, handling phrases like
        'from', 'to', 'in', 'through', 'until', 'since', and complex dates in multiple formats.
        Parameters:
        text (str): The input string containing time periods.
        Returns:
        Dict[str, Optional[str]]: A dictionary containing the 'from' and 'to' periods with their units.
                                Returns None if a period or unit is not found.
        """
        # Define regex patterns for quarters and months
        #print("\nTEXT I'm checking : ",text)
        text = text.lower()
        year_pattern = r"(20[1-2][0-9]|'[1-2][0-9])"
        semester_pattern = r"(s[1-2] \d{4}|s[1-2] '[1-2][0-9]|\d{4} s[1-2]|h[1-2] \d{4}|\d{4} h[1-2]|h[1-2] '[1-2][0-9]|(?:first|second) half of \d{4}|s[1-2]|h[1-2])"
        semester_pattern1 = r"(s[1-2] |s[1-2] '[1-2][0-9]| s[1-2]|h[1-2] | h[1-2]|h[1-2] '[1-2][0-9]|(?:first|second) half of |s[1-2]|h[1-2])"
        quarter_pattern = r"(q[1-4] \d{4}|\d{4} q[1-4]|(?:first|second|third|fourth) quarter of \d{4}|(?:1st|2nd|3rd|4th) quarter of \d{4}|q[1-4])"
        quarter_pattern1 = r"(q[1-4]|q[1-4]|(?:first|second|third|fourth) quarter of|(?:1st|2nd|3rd|4th) quarter of|q[1-4])"
        month_pattern = r"((?:jan |january |feb |february |mar |march |apr |april |may |jun |june |jul |july |aug |august |sep |september |oct |october |nov |november |dec |december )\d{4}|(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|september|oct|october|nov|november|dec|december) '[-2][0-9]|(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|september|oct|october|nov|november|dec|december)'[0-9]{2}|(?:jan |january |feb |february |mar |march |apr |april |may |jun |june |jul |july |aug |august |sep |september |oct |october |nov |november |dec |december ))"
        month_pattern1 = r"((?:jan |january |feb |february |mar |march |apr |april |may |jun |june |jul |july |aug |august |sep |september |oct |october |nov |november |dec |december )|(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|september|oct|october|nov|november|dec|december) '[-2][0-9]|(?:jan|january|feb|february|mar|march|apr|april|may|jun|june|jul|july|aug|august|sep|september|oct|october|nov|november|dec|december)'[0-9]{2}|(?:jan |january |feb |february |mar |march |apr |april |may |jun |june |jul |july |aug |august |sep |september |oct |october |nov |november |dec |december ))"
        date_pattern = r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})"
        relative_pattern = r"(last (\d+) (week|month|quarter|semester|year)s?|latest (\d+) (week|month|quarter|semester|year)s?|recent (\d+) (week|month|quarter|semester|year)s?|previous (\d+) (week|month|quarter|semester|year)s?|r(\d+)(m|y|q)|rolling (\d+) (week|month|quarter|semester|year)s?)"
        relative_pattern_current = r"(last (week|month|quarter|semester|year)s?|latest (week|month|quarter|semester|year)s?|recent (week|month|quarter|semester|year)s?|this (week|month|quarter|semester|year)s?|previous (week|month|quarter|semester|year)s?|current (week|month|quarter|semester|year)s?)"
        exclusion_pattern = r"(?:excluding|without|not having|exclude)\s+(.+)"
         
        exclusion_matches = re.findall(exclusion_pattern, text)

        # Normalize quarter phrases (e.g., "first quarter of 2024" -> "Q1 2024")
        text = re.sub(r"(\d{4})\s+(Q[1-4])", r"\2 \1", text)
        text = re.sub(r"first quarter of (\d{4})", r"Q1 \1", text, flags=re.IGNORECASE)
        text = re.sub(r"second quarter of (\d{4})", r"Q2 \1", text, flags=re.IGNORECASE)
        text = re.sub(r"third quarter of (\d{4})", r"Q3 \1", text, flags=re.IGNORECASE)
        text = re.sub(r"fourth quarter of (\d{4})", r"Q4 \1", text, flags=re.IGNORECASE)
        text = re.sub(r"first half of (\d{4})", r"S1 \1", text, flags=re.IGNORECASE)
        text = re.sub(r"second half of (\d{4})", r"S2 \1", text, flags=re.IGNORECASE)
        rolling_pattern = r'rolling (\d+)'
        rolling_matches = re.findall(rolling_pattern, text, re.IGNORECASE)
        if rolling_matches:
            for rolling in rolling_matches:
                text = text.replace(f"rolling {rolling}", "")


        word_before_average_pattern = r"(\w+)\s+(?:average|avg)"
        word_before_average_matches = re.findall(word_before_average_pattern, user_input, re.IGNORECASE)
        if word_before_average_matches:
            for words in word_before_average_matches:
                text = text.replace(f"{words} average", "")
                text = text.replace(f"{words} avg", "")


        if exclusion_matches:
            for exclusion in exclusion_matches:
                text = text.replace(f"excluding {exclusion}", "")
        print("TEXT IM CHECKING : ",text)
        # Find all patterns in the input text
        years = re.findall(year_pattern, text)
        semesters = re.findall(semester_pattern, text)
        quarters = re.findall(quarter_pattern, text)
        months = re.findall(month_pattern, text)
        print("\nquarters I'm checking: ",quarters)
        print("\nyears I'm checking: ",years)
        print("\nmonths I'm checking: ",months)
        print("\nsemesters I'm checking: ",semesters)
        # print("xxxx", months)
        dates = re.findall(date_pattern, text)
        relative_periods = re.findall(relative_pattern, text)
        relative_periods_current = re.findall(relative_pattern_current, text)
        #exclusions = []
        #exclusion_matches = re.findall(exclusion_pattern, text)
        print("\nExclusion Matches :")
        print(exclusion_matches)
        print("\nExclusion Pattern :")
        print(exclusion_pattern)

        # Initialize the result dictionary
        result = {
            'from_period': None,
            'from_unit': None,
            'to_period': None,
            'to_unit': None,
            'relative_unit': None,
            'relative_number': None,
            "excluded_periods" : [] #NEW ADDITION FOR TIME PERIOD EXCLUSION
        }
        def format_year(year: str) -> str:
            """Formats a year as 'YYYY'."""
            if len(year) == 4:
                year = year
            else:
                year = "20"+year[-2:]
            return f"{year}"
        def format_quarter(quarter: str, year: str) -> str:
            """Formats a quarter and year as 'Qx YYYY'."""
            year = format_year(year)
            quarter = quarter.upper()
            return f"{year}-{quarter}"
        def format_semester(semester: str, year: str) -> str:
            """Formats a semester and year as 'Sx YYYY'."""
            year = format_year(year)
            semester = re.sub(r"h", r"s", semester, flags=re.IGNORECASE)
            return f"{semester} {year}"
        def format_month(month: str, year: str) -> str:
            """Formats a month and year as 'MMM YYYY'."""
            year = format_year(year)
            month = month_string_to_number(month[:3])

            return f"{year}{month}"
        def parse_date(date_str: str) -> str:
            """Parses a date string into 'YYYY-MM-DD' format."""
            try:
                return parse(date_str).strftime('%Y-%m-%d')
            except ValueError:
                return date_str
        def get_time_filters(results):
            time_filter = ''
            if relative_periods:
                period = "r"+results("relative_number")+ results("relative_unit")[0]
                #code for adding excluding timeperiods to the timeperiod['excluded periods"] section
        for exclusion in exclusion_matches:
            print("Exclusion : ",exclusion)
            if re.findall(quarter_pattern, exclusion): #NEW ADDITION FOR QUARTER EXCLUSION
                qp = re.findall(quarter_pattern1, exclusion)
                #print("Crossed If")  # Handle "Q1 2023" format
                yp = re.findall(year_pattern, exclusion)
                for quarter, year in zip(qp, yp):
                    print("Processing Quarter:", quarter, "Year:", year)
                    result['excluded_periods'].append(format_quarter(quarter,year))

            elif re.findall(semester_pattern, exclusion): # Handle "S1 2023" format
                sp = re.findall(semester_pattern1, exclusion)
                yp = re.findall(year_pattern, exclusion)
                #semester_match = re.search( r"(s[1-2]|s[1-2] '[1-2][0-9]|s[1-2]|h[1-2]|h[1-2]|h[1-2] '[1-2][0-9]|(?:first|second) half of|s[1-2]|h[1-2])", exclusion)
                for semester, year in zip(sp, yp):
                    print("Processing Semester:", semester, "Year:", year)
                    result['excluded_periods'].append(format_semester(semester,year)) #NEW ADDITION FOR SEMESTER EXCLUSION

            elif re.findall(month_pattern, exclusion):  # Handle "August 2023" format
                mp = re.findall(month_pattern1, exclusion)
                yp = re.findall(year_pattern, exclusion)
                
                for month, year in zip(mp, yp):
                    print("Processing Month:", month, "Year:", year)
                    result['excluded_periods'].append(format_month(month,year)) #NEW ADDITION FOR MONTH_YEAR EXCLUSION

            elif re.findall(year_pattern, exclusion):
                
                result['excluded_periods'].append(format_year(exclusion))

            elif re.match(date_pattern, exclusion):  # Handle specific date exclusions
                result['excluded_periods'].append(parse_date(exclusion))


        #semester_pattern = r"(s[1-2] \d{4}|s[1-2] '[1-2][0-9]|\d{4} s[1-2]|h[1-2] \d{4}|\d{4} h[1-2]|h[1-2] '[1-2][0-9]|(?:first|second) half of \d{4}|s[1-2]|h[1-2])"


        
        
        
        # Handle complex relative periods like "last 6 months"
        if relative_periods:
            amount, unit = int(relative_periods[0][0].split()[1]), relative_periods[0][0].split()[2]
            result['relative_number'] = amount
            result['relative_unit'] = unit
        elif relative_periods_current:
            amount, unit = int(1), relative_periods_current[0][0].split()[1]
            result['relative_number'] = amount
            result['relative_unit'] = unit
        # Handle cases where both "from" and "to" share the same year
        elif len(quarters) >= 2 and "from" in text and "to" in text:
            if len(re.findall(year_pattern, quarters[0])) > 0:
                year = re.findall(year_pattern, quarters[0])[0]
            else:
                if years:
                    year = years[0]
                else:
                    year = 2024  # Default it to 2024
            quarter = re.findall(quarter_pattern, re.sub(year_pattern, "", quarters[0]))
            result['from_period'] = format_quarter(quarter[0], year)
            if len(re.findall(year_pattern, quarters[1])) > 0:
                year = re.findall(year_pattern, quarters[1])[0]
            else:
                if years:
                    year = years[0]
                else:
                    year = 2024  # # Default it to 2024
            quarter = re.findall(quarter_pattern, re.sub(year_pattern, "", quarters[1]))
            result['to_period'] = format_quarter(quarter[0], year)
            result['from_unit'], result['to_unit'] = 'quarter', 'quarter'
        elif len(semesters) >= 2 and "from" in text and "to" in text:
            if len(re.findall(year_pattern, semesters[0])) > 0:
                year = re.findall(year_pattern, semesters[0])[0]
            else:
                if years:
                    year = years[0]
                else:
                    year = 2024  # Default it to 2024
            semester = re.findall(semester_pattern, re.sub(year_pattern, "", semesters[0]))
            result['from_period'] = format_semester(semester[0], year)
            if len(re.findall(year_pattern, semesters[1])) > 0:
                year = re.findall(year_pattern, semesters[1])[0]
            else:
                if years:
                    year = years[0]
                else:
                    year = 2024  # # Default it to 2024
            semester = re.findall(semester_pattern, re.sub(year_pattern, "", semesters[1]))
            result['to_period'] = format_semester(semester[0], year)
            result['from_unit'], result['to_unit'] = 'semester', 'semester'
        elif len(months) >= 2 and "from" in text and "to" in text:
            if len(re.findall(year_pattern, months[0])) > 0:
                year = re.findall(year_pattern, months[0])[0]
            
            else:
                if years:
                    year = years[0]
                else:
                    year = 2024  # Default it to 2024
            month = re.findall(month_pattern, re.sub(year_pattern, "", months[0]))
            
            result['from_period'] = format_month(month[0], year)
            if len(re.findall(year_pattern, months[1])) > 0:
                year = re.findall(year_pattern, months[1])[0]
            else:
                if years:
                    year = years[0]
                else:
                    year = 2024  # # Default it to 2024
            month = re.findall(month_pattern, re.sub(year_pattern, "", months[1]))
            
            result['to_period'] = format_month(month[0], year)
            result['from_unit'], result['to_unit'] = 'month', 'month'
        # Handle other cases based on the input text
        elif " from " in text or " since " in text:
            if " to " in text or " through " in text or " until " in text:
                if quarters and months:
                # Check if 'from' aligns with a quarter or a month
                    if text.index("from") < text.index("to"):
                        # Handle 'from quarter to month'
                        if text.index(quarters[0]) < text.index(months[0]):
                            result['from_period'] = format_quarter(quarters[0].split()[0], quarters[0].split()[1])
                            result['to_period'] = format_month(months[-1].split()[0], months[-1].split()[1])
                            result['from_unit'], result['to_unit'] = 'quarter', 'month'
                        else:
                            # Handle 'from month to quarter'
                            result['from_period'] = format_month(months[0].split()[0], months[0].split()[1])
                            result['to_period'] = format_quarter(quarters[-1].split()[0], quarters[-1].split()[1])
                            result['from_unit'], result['to_unit'] = 'month', 'quarter'


                elif semesters:
                    result['from_period'], result['to_period'] = format_semester(semesters[0].split()[0], semesters[0].split()[1]), format_semester(semesters[-1].split()[0], semesters[-1].split()[1])
                    result['from_unit'], result['to_unit'] = 'semester', 'semester'
                elif quarters:
                    result['from_period'], result['to_period'] = format_quarter(quarters[0].split()[0], quarters[0].split()[1]), format_quarter(quarters[-1].split()[0], quarters[-1].split()[1])
                    result['from_unit'], result['to_unit'] = 'quarter', 'quarter'
                elif months:
                    result['from_period'], result['to_period'] = format_month(months[0].split()[0],months[0].split()[1]), format_month(months[-1].split()[0],months[-1].split()[1])
                    result['from_unit'], result['to_unit'] = 'month', 'month'
                elif dates:
                    result['from_period'], result['to_period'] = parse_date(dates[0]), parse_date(dates[-1])
                    result['from_unit'], result['to_unit'] = 'date', 'date'
                elif years:
                    result['from_period'], result['to_period'] = format_year(years[0]), format_year(years[1])
                    result['from_unit'], result['to_unit'] = 'year', 'year'






                
                




            else:
                if semesters:
                    result['from_period'] = format_semester(semesters[0].split()[0], semesters[0].split()[1])
                    result['from_unit'] = 'semester'
                elif quarters:
                    result['from_period'] = format_quarter(quarters[0].split()[0], quarters[0].split()[1])
                    result['from_unit'] = 'quarter'
                elif months:
                    result['from_period'] = format_month(months[0].split()[0],months[0].split()[1])
                    result['from_unit'] = 'month'
                elif dates:
                    result['from_period'] = parse_date(dates[0])
                    result['from_unit'] = 'date'
                elif years:
                    result['from_period'] = format_year(years[0])
                    result['from_unit'] = 'year'
        elif " in " in text or " for " in text:
            if semesters:
                result['from_period'] = result['to_period'] = format_semester(semesters[0].split()[0], semesters[0].split()[1])
                result['from_unit'] = result['to_unit'] = 'semester'
            elif quarters:
                result['from_period'] = result['to_period'] = format_quarter(quarters[0].split()[0], quarters[0].split()[1])
                result['from_unit'] = result['to_unit'] = 'quarter'
            elif months:
                result['from_period'] = result['to_period'] = format_month(months[0].split()[0],months[0].split()[1])
                result['from_unit'] = result['to_unit'] = 'month'
            elif dates:
                date = parse_date(dates[0])
                result['from_period'] = result['to_period'] = date
                result['from_unit'] = result['to_unit'] = 'date'
            elif years:
                result['from_period'] = result['to_period'] = format_year(years[0])
                result['from_unit'] = result['to_unit'] = 'year'
        elif " until " in text or " through " in text:
            if years:
                result['to_period'] = format_year(years[0])
                result['to_unit'] = 'year'
            if semesters:
                result['to_period'] = format_semester(semesters[-1].split()[0], semesters[-1].split()[1])
                result['to_unit'] = 'semester'
            elif quarters:
                result['to_period'] = format_quarter(quarters[-1].split()[0], quarters[-1].split()[1])
                result['to_unit'] = 'quarter'
            elif months:
                result['to_period'] = format_month(months[-1].split()[0],months[-1].split()[1])
                result['to_unit'] = 'month'
            elif dates:
                result['to_period'] = parse_date(dates[-1])
                result['to_unit'] = 'date'
        return result
    #print("\nUSER INPUT : ",user_input)
    final_selection['timeperiod'] = extract_time_periods(user_input)
    
    # print(final_selection)
    print("\nfinal selection after time period : ")
    print(final_selection)
    return final_selection

def fn_primary_function(user_input, metrics_dict, brand_dict, custom_dict,
                       timeperiod_dict, channel_dict, indication_dict, controller_dict, pbm_dict,rmo_dict, benefittype_dict,
                       formulary_dict, market_dict, market_products_dict, operator_dict, visualization_dict, splitby_dict, grouping_dict, datasource_dict, thresholds):
    stopWords = set(stopwords.words('english')).union({"what", "why", ",", ":", ".", "and"})
    words = word_tokenize(user_input)
    excluding_flag = False
    primarysearch = []
    timeperiodfrom = timeperiodto = ''
    excluding_timeperiodto=excluding_timeperiodfrom = ' '
    from_count = to_count = by_count = ly_count = 0
    for w in words:
        w_lower = w.lower()
        if w_lower not in stopWords:
           primarysearch.append(w)
        
        # Count occurrences of context-related keywords
        if w_lower == 'by':
            by_count += 1
        if w_lower == 'from':
            from_count += 1
        if fn_right(w, 2).lower() == 'ly':
            ly_count += 1
        if w_lower in {'to', 'till', 'until', 'through'}:
            to_count += 1
        
        # new addtion Handle "excluding" logic
        if w_lower == 'excluding':
            excluding_flag = True
            continue  # Skip processing for the word "excluding" itself
        
        if excluding_flag:
            # new addition Capture 'from' date for excluding context
            if from_count > 0 and to_count == 0 and by_count == 0 and w_lower not in stopWords:
                excluding_timeperiodfrom = str(excluding_timeperiodfrom) + w
            
            # Capture 'to' date for excluding context
            if from_count > 0 and to_count > 0 and by_count == 0 and w_lower not in stopWords:
                excluding_timeperiodto = str(excluding_timeperiodto) + w
            continue
        
        # Handle primary context
        
        # Capture 'from' date for primary context
        if from_count > 0 and to_count == 0 and by_count == 0 and w_lower not in stopWords:
            timeperiodfrom = str(timeperiodfrom) + w
        
        # Capture 'to' date for primary context
        if from_count > 0 and to_count > 0 and by_count == 0 and w_lower not in stopWords:
            timeperiodto = str(timeperiodto) + w

    # Finalize time period strings
    timeperiodfrom = timeperiodfrom.strip()
    timeperiodto = timeperiodto.strip()
    excluding_timeperiodfrom = excluding_timeperiodfrom.strip()
    excluding_timeperiodto = excluding_timeperiodto.strip()
    
    if excluding_timeperiodfrom:
        timeperiodfrom = f"{timeperiodfrom} excluding {excluding_timeperiodfrom}".strip()
    if excluding_timeperiodto:
        timeperiodto = f"{timeperiodto} excluding {excluding_timeperiodto}".strip()

    # Remove stop words from primary search words
    primarysearchwords = [w for w in primarysearch if w.lower() not in stopWords]
    print("\nTime period from  :")
    print(timeperiodfrom)
    print("\nTime period to  :")
    print(timeperiodto)
    print("\nprimary searc hwords : ")
    print(primarysearchwords)
    # Generate the final selection dictionary
    final_selection = fn_final_selection(
        timeperiodfrom,
        timeperiodto,
        user_input,
        primarysearchwords,
        market_dict,
        market_products_dict,
        grouping_dict,
        datasource_dict
    )
    
    return final_selection






# --------------------- Core Functions ---------------------

def clean_inputs(clean_inputs_dict, input_master_dict=master_dict):
    # print('------------------- <<<< PARSING INPUTS >>>> ---------------------------')

    # Initialize variables with default values



    # metric_names = clean_inputs_dict.get('metric_names', [input_master_dict['other_constants']['product_name']['inflamm']])
    # if isinstance(metric_names, str):
    #     metric_names = [metric_names]
    # elif not isinstance(metric_names, list):
    #     raise Exception("metric_names should be a list.")

    # metric_types = clean_inputs_dict.get('metric_types', input_master_dict['other_constants']['metric_type'])
    # if isinstance(metric_types, str):
    #     metric_types = [metric_types]
    # elif not isinstance(metric_types, list):
    #     raise Exception("metric_types should be a list.")

    # if metric_types==[] or metric_types==['']:
    #   metric_types=['trx']



    # for mt in metric_types:
    #     if mt not in ['trx', 'nrx', 'nbrx']:
    #         raise Exception("Metric type incompatible. Available metric types are: trx, nrx, nbrx")

    metrics = clean_inputs_dict.get('metrics', [input_master_dict['other_constants']['product_name']['inflamm']])
    if isinstance(metrics, str):
        metrics = [metrics]
    elif not isinstance(metrics, list):
        raise Exception("metrics should be a list.")

    excluded_metrics = clean_inputs_dict.get('excluded_metrics', [input_master_dict['other_constants']['product_name']['inflamm']])

    if isinstance(excluded_metrics, str):
        excluded_metrics = [excluded_metrics]
    elif not isinstance(excluded_metrics, list):
        raise Exception("excluded_metrics should be a list.")

    def get_key(dict, value):
        return_val = None
        for key, val in dict.items():
            if value in dict[key]:
                return_val = key
        return return_val

    if len(clean_inputs_dict.get('data_source_names'))>0:
        datasource = clean_inputs_dict.get('data_source_names')[0]
    else:
        datasource = get_key(datasource_metrics_dict, metrics[0])




    #Error for tomorrow
    if len(clean_inputs_dict.get('excluded_data_source_names'))>0:
        excluded_datasource = clean_inputs_dict.get('exclued_data_source_names')[0]
    else:
        excluded_datasource = get_key(datasource_metrics_dict, metrics[0])

    #NEW ADDITION MARKET NAME INCLUSION
    market_name = clean_inputs_dict.get('market_name', input_master_dict['other_constants']['market_name'])
    if isinstance(market_name, str):
        market_name = [market_name]
    elif not isinstance(market_name, list):
        raise Exception("market_name should be a list.")
    
    excluded_market_name = clean_inputs_dict.get('excluded_market_name', input_master_dict['other_constants']['market_name'])

    if isinstance(excluded_market_name, str):
        excluded_market_name = [excluded_market_name]
    elif not isinstance(excluded_market_name, list):
        raise Exception("excluded_market_name should be a list.")

    print("Market name :",market_name)

    # input_master_dict['datamart_table_names'][datasource]
    for m in market_name:
        if m not in input_master_dict['other_constants']['product_name']:
            raise Exception(f"Market name not found in datamart table names.")
    #Check here
    #if excluded_market_name not in input_master_dict['other_constants']['product_name']:
        #raise Exception(f"Market name '{excluded_market_name}' not found in datamart table names.")
    product_names= []
    excluded_product_names= []
    for m in market_name:
        product_names = clean_inputs_dict.get('product_names', [input_master_dict['other_constants']['product_name'][m]])
        excluded_product_names = clean_inputs_dict.get('excluded_product_names', [input_master_dict['other_constants']['product_name'][m]])
    
    #NEW ADDITION
    if isinstance(product_names, str):
        product_names = [product_names]
    elif not isinstance(product_names, list):
        raise Exception("product_names should be a list.")
    product_names = [x.upper().strip() for x in product_names]

    if isinstance(excluded_product_names, str):
        excluded_product_names = [excluded_product_names]
    elif not isinstance(excluded_product_names, list):
        raise Exception("Excluded product_names should be a list.")
    excluded_product_names = [x.upper().strip() for x in product_names]

    start_date = clean_inputs_dict.get('start_date', input_master_dict['other_constants']['start_date'])

    if start_date=='':
      start_date=input_master_dict['other_constants']['start_date']

    end_date = clean_inputs_dict.get('end_date', input_master_dict['other_constants']['end_date'])

    if end_date=='':
      end_date=input_master_dict['other_constants']['end_date']

    metric_frequency = clean_inputs_dict.get('metric_frequency', input_master_dict['other_constants']['metric_frequency'])
    #if metric_frequency not in ['q', 'm', 'y', 'd']:
    #    raise Exception("Input frequency type not valid. Please choose one of: q (Quarterly), m (Monthly), y (Yearly), d (Daily)")



    # Handle optional filters
    def process_filter(key):
        value = clean_inputs_dict.get(key, [])
        if isinstance(value, str):
            value = [value]
        elif not isinstance(value, list):
            raise Exception(f"{key} should be a list.")
        return split_list_to_string([x.upper().strip() for x in value]) if value else ''

    channel_names = process_filter('channel_names')
    product_names = process_filter('product_names')
    controller_names = process_filter('controller_names')
    pbm_names = process_filter('pbm_names')
    rmo_names = process_filter('rmo_names')
    indication_names = process_filter('indication_names')
    benefittype_names = process_filter('benefittype_names')
    grouping_names = clean_inputs_dict.get('grouping_names', [])

    excluded_channel_names = process_filter('excluded_channel_names')
    excluded_product_names = process_filter('excluded_product_names')
    excluded_controller_names = process_filter('excluded_controller_names')
    excluded_pbm_names = process_filter('excluded_pbm_names')
    excluded_rmo_names = process_filter('excluded_rmo_names')
    excluded_indication_names = process_filter('excluded_indication_names')
    excluded_benefittype_names = process_filter('excluded_benefittype_names')
    excluded_grouping_names = clean_inputs_dict.get('excluded_grouping_names', [])

    cut_by = clean_inputs_dict.get('cut_by', [])
    if isinstance(cut_by, str):
        cut_by = [cut_by]
    elif not isinstance(cut_by, list):
        raise Exception("cut_by should be a list.")

    excluded_cut_by = clean_inputs_dict.get('excluded_cut_by', [])
    if isinstance(excluded_cut_by, str):
        excluded_cut_by = [excluded_cut_by]
    elif not isinstance(excluded_cut_by, list):
        raise Exception("excluded_cut_by should be a list.")

    partition_cut_by = clean_inputs_dict.get('partition_cut_by', [])
    if isinstance(excluded_cut_by, str):
        excluded_cut_by = [excluded_cut_by]
    elif not isinstance(excluded_cut_by, list):
        raise Exception("excluded_cut_by should be a list.")


    for m in market_name:
        if m not in input_master_dict['datamart_table_names'][datasource]:
            datamart_table_name = input_master_dict['datamart_table_names'][datasource]['all']
        else:
            datamart_table_name = input_master_dict['datamart_table_names'][datasource][m]

    return (metrics, market_name, product_names, start_date, end_date, metric_frequency,
            channel_names, controller_names, pbm_names, rmo_names, benefittype_names, indication_names, grouping_names,cut_by, datasource, datamart_table_name, clean_inputs_dict.get('timeperiod'),
           excluded_metrics, excluded_market_name, excluded_product_names,excluded_channel_names, excluded_controller_names,
            excluded_pbm_names, excluded_rmo_names, excluded_benefittype_names, excluded_indication_names, excluded_grouping_names,
            excluded_cut_by, excluded_datasource,partition_cut_by)




def create_sql(text,
               metrics,
               market_name,
               product_names,
               start_date,
               end_date,
               metric_frequency,
               channel_names,
               controller_names,
               pbm_names,
               rmo_names,
               benefittype_names,
               indication_names,
               grouping_names,
               cut_by,
               datasource,
               datamart_table_name,
               timeperiod,
               excluded_metrics,
               excluded_market_name,
               excluded_product_names,
               excluded_channel_names,
               excluded_controller_names,
               excluded_pbm_names,
               excluded_rmo_names,
               excluded_benefittype_names,
               excluded_indication_names,
               excluded_grouping_names,
               excluded_cut_by,
               excluded_datasource,
               partition_cut_by,
               input_master_dict=master_dict
):

    top_pattern = r'top (\d+)'
    top_matches = re.findall(top_pattern, text, re.IGNORECASE)
    #top_matches = top_matches_raw[0].split() if top_matches_raw else []
    top_matches1 = [match.lower() for match in top_matches]
    


    top_pattern_with_word = r'top (\d+)\s+([\w\s]+?)(?=\s+by|\s*$)'
    top_matches_with_word = re.findall(top_pattern_with_word, text, re.IGNORECASE)
    # Extract just the word after "top n" and check if it's in cut_by
    words_after_top = [match[1].strip() for match in top_matches_with_word]

    cut_by_for_partition=[]
    cut_by_for_contribution=[]
    if len(cut_by)>1:
        cut_by_for_partition = [word for word in cut_by if word not in partition_cut_by]
    
    cut_by_for_contribution = [word for word in cut_by if word in cut_by_for_partition]




    rolling_pattern = r'rolling (\d+)'
    rolling_matches = re.findall(rolling_pattern, text, re.IGNORECASE)
    #top_matches = top_matches_raw[0].split() if top_matches_raw else []
    rolling_matches1 = [match.lower() for match in rolling_matches]
    if rolling_matches:
        rolling_value = int(rolling_matches[0])


    rolling_pattern_with_word = r'rolling (\d+)\s+(\w+)'
    rolling_matches_with_word = re.findall(rolling_pattern_with_word, text, re.IGNORECASE)

    # Extract just the word after "top n" and check if it's in cut_by
    words_after_rolling = [match[1] for match in rolling_matches_with_word]




    print("\nTop Matches with Words:", top_matches_with_word)
    print("\nWords After Top:", words_after_top)
   # print("\nCut By For Partition top N :", cut_by_for_partition)

    word_before_average_pattern = r"(\w+)\s+(?:average|avg)"
    word_before_average_matches = re.findall(word_before_average_pattern, text, re.IGNORECASE)
    #word_before_average_matches_lower = [match.lower() for match in word_before_average_matches]

    average_month_check=["monthly","month","Month","Monthly"]
    average_quarter_check=["quarterly","quarter","Quarter","Quarterly"]
    average_year_check=["yearly","year","Year","Yearly"]
    filtered_matches_month = [word for word in word_before_average_matches if word.lower() in average_month_check]
    filtered_matches_quarter = [word for word in word_before_average_matches if word.lower() in average_quarter_check]
    filtered_matches_year = [word for word in word_before_average_matches if word.lower() in average_year_check]
    #word_before_average_matches_month = [match.lower() for match in rolling_matches]
    if word_before_average_matches:
        for words in word_before_average_matches:
            text = text.replace(f"{words} average", "")
            text = text.replace(f"{words} avg", "")

    if "yoy" in text.lower():  # Use .lower() for case-insensitive checking
        yoy_flag=True
        print("\n'yoy' is present in the text.")
    else:
        yoy_flag=False
        print("\n'yoy' is not present in the text.")


    print("Partition Cut by in create SQL : ",partition_cut_by)


    previous_pattern = r"(previous (\d+) (week|month|quarter|semester|year)s?)"
    previous_pattern_current = r"(previous (week|month|quarter|semester|year)s?)"
    previous_periods = re.findall(previous_pattern, text)
    previous_periods_current = re.findall(previous_pattern_current, text)
    previous_flag = False

    if previous_periods or previous_periods_current:
        previous_flag = True



    
    


    # print('------------------- <<<< CREATING SQL MAGIC >>>> ---------------------------')

    # Get market-specific data
    #market_data = input_master_dict['market_specific'].get(market_name)
    #if not market_data:
    #    raise ValueError(f"Market '{market_name}' is not defined in input_master_dict['market_specific'].")


    for m in market_name:
    
        if m in input_master_dict['sql_colnames'][datasource]:
            sql_colnames = input_master_dict['sql_colnames'][datasource][m]
        else:
            sql_colnames = input_master_dict['sql_colnames'][datasource]['all']

    if datasource=='laad_weekly':
        metric_businessrules = input_master_dict['metric_businessrules']['laad']
    else:
        
        metric_businessrules = input_master_dict['metric_businessrules'][datasource]

    if datasource in input_master_dict['universal_filters']:
        universal_filters = input_master_dict['universal_filters'][datasource]
    elif datasource=='laad_weekly':
        universal_filters = input_master_dict['universal_filters']['laad']
    else:
        universal_filters=None
    print("Universal_Filters : ",universal_filters)

    max_n = 0
    #if sql_colnames['timeperiod_week_colname'] :
     #   lowest_granularity = sql_colnames['timeperiod_week_colname']
      #  max_n = 53
    if sql_colnames['timeperiod_month_colname'] :
        lowest_granularity = sql_colnames['timeperiod_month_colname']
        max_n = 13
    elif sql_colnames['timeperiod_quarter_colname'] :
        lowest_granularity = sql_colnames['timeperiod_quarter_colname']
        max_n = 5
    elif sql_colnames['timeperiod_year_colname'] :
        lowest_granularity = sql_colnames['timeperiod_year_colname']
        max_n = 2

    print("LOWEST GRANULARITY IN CREATE SQL : ", lowest_granularity)

    if sql_colnames['timeperiod_year_colname'] :
        highest_granularity = sql_colnames['timeperiod_year_colname']
    elif sql_colnames['timeperiod_quarter_colname'] :
        highest_granularity = sql_colnames['timeperiod_quarter_colname']
    elif sql_colnames['timeperiod_month_colname'] :
        highest_granularity = sql_colnames['timeperiod_month_colname']

    print("LOWEST GRANULARITY IN CREATE SQL : ", highest_granularity)



    # Build initial dimensions and group_by_columns
    dimensions = []
    dimensions_top_n = []
    group_by_columns = []
    group_by_columns_top_n = []
    growth_partition=[]
    cut_by_rolling = []
    where_conditions = []
    where_conditions_top_n = []
    denom_where_conditions_top_n=[]
    denom_where_conditions = []
    time_period_growth=''
    time_col = sql_colnames['timeperiod_month_colname']
    time_dim = ''
    # Adjust time dimension based on frequency
    if metric_frequency == 'q':
        time_dim = sql_colnames['timeperiod_quarter_colname']
    elif metric_frequency == 'y':
        time_dim = sql_colnames['timeperiod_year_colname']
    elif metric_frequency == 'm':
        time_dim = sql_colnames['timeperiod_month_colname']
    elif metric_frequency == 'd':
        time_dim = sql_colnames['fill_date_colname']
    # else:
    #     time_dim = sql_colnames['timeperiod_month_colname']  # default
    if time_dim != '':
        dimensions.append(time_dim)
        group_by_columns.append(time_dim)
        dimensions_top_n.append(time_dim)
        group_by_columns_top_n.append(time_dim)


    # Handle 'cut_by' dimensions
    if cut_by:
        # Replace cut_by dimensions with actual column names from sql_colnames if necessary
        cut_by_columns = [sql_colnames.get(dim + '_colname', sql_colnames.get(dim, dim)) for dim in cut_by]
        dimensions.extend(cut_by_columns)
        dimensions_top_n.extend(cut_by_columns)
        growth_partition.extend(cut_by_columns)
        group_by_columns.extend(cut_by_columns)
        group_by_columns_top_n.extend(cut_by_columns)
        cut_by_rolling.extend(cut_by_columns)
        #where_conditions_top_n.extend([f"a.{col} IS NOT NULL" for col in cut_by_columns])
    

    if cut_by_for_contribution:
        contribution_cut_by_colunm = ["a." + sql_colnames.get(dim + '_colname', sql_colnames.get(dim, dim)) for dim in cut_by_for_contribution]
        contribution_rank = ','.join(contribution_cut_by_colunm)


    
    if cut_by_for_partition:
        # Replace cut_by dimensions with actual column names from sql_colnames if necessary
        partition_cut_by_colunm = ["a." + sql_colnames.get(dim + '_colname', sql_colnames.get(dim, dim)) for dim in cut_by_for_partition]
        partition_rank = ','.join(partition_cut_by_colunm)
    #partition_rank.extend(partition_cut_by_colunm)
    #group_by_columns_rank.extend(partition_cut_by_colunm)
    #print("Partition_Rank Check: ",partition_rank)
    #print("cut_by_rolling : ",cut_by_rolling)

    cut_by_rolling_code = ", ".join([f"a.{cut}" for cut in cut_by_rolling])

    # Always include brand_colname
    brand_col = sql_colnames['brand_colname']
    if brand_col not in dimensions:
        dimensions.append(brand_col)
        growth_partition.append(brand_col)
    if brand_col not in group_by_columns:
        group_by_columns.append(brand_col)

    # Collect additional group by columns from all metrics
    for m_name in metrics:
        metric_rules_dict = metric_businessrules[m_name]
        def get_key(dict, value):
            for key, val in dict.items():
                if key == value:
                    return dict[key]
            return dict['all']  #
        
        
        metric_rules = get_key(metric_rules_dict, market_name)
        if not metric_rules:
            raise ValueError(f"Metric '{m_name}' with type is not defined in the metric_businessrules.")
        additional_group_by_keys = metric_rules.get('additional_group_by', [])
        additional_group_by_cols = [sql_colnames.get(key, key) for key in additional_group_by_keys]
        for col in additional_group_by_cols:
            if col not in dimensions:
                dimensions.append(col)
                growth_partition.append(col)
            if col not in group_by_columns:
                group_by_columns.append(col)

    # Build WHERE clause for main query

    
    timeperiod_filters_get = get_key(timeperiod_filters[datasource], market_name)
    # Initialize WHERE conditions
    if datasource == 'laad' or datasource.lower() == 'npa' or datasource.lower() == 'mmit' or datasource.lower() == 'mmit_change':
      if timeperiod['relative_unit']!=None:
        if not previous_flag:
            where_conditions.append(timeperiod_filters_get["r"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
            if timeperiod['relative_unit'][0] == 'm':
                time_period_growth = 'month_year'
                dimensions.append(sql_colnames['timeperiod_month_colname'])
                group_by_columns.append(sql_colnames['timeperiod_month_colname'])
            elif timeperiod['relative_unit'][0] == 'q':
                time_period_growth = 'quarter'
                dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
            elif timeperiod['relative_unit'][0] == 'y':
                time_period_growth = 'year'
                dimensions.append(sql_colnames['timeperiod_year_colname'])
                group_by_columns.append(sql_colnames['timeperiod_year_colname'])
            denom_where_conditions.append(timeperiod_filters_get["r"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
            where_conditions_top_n.append(timeperiod_filters_get["r"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
            denom_where_conditions_top_n.append(timeperiod_filters_get["r"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
        else:
            where_conditions.append(timeperiod_filters_get["p"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
            if timeperiod['relative_unit'][0] == 'm':
                time_period_growth = 'month_year'
                dimensions.append(sql_colnames['timeperiod_month_colname'])
                group_by_columns.append(sql_colnames['timeperiod_month_colname'])
            elif timeperiod['relative_unit'][0] == 'q':
                time_period_growth = 'quarter'
                dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
            elif timeperiod['relative_unit'][0] == 'y':
                time_period_growth = 'year'
                dimensions.append(sql_colnames['timeperiod_year_colname'])
                group_by_columns.append(sql_colnames['timeperiod_year_colname'])
            denom_where_conditions.append(timeperiod_filters_get["p"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
            where_conditions_top_n.append(timeperiod_filters_get["p"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
            denom_where_conditions_top_n.append(timeperiod_filters_get["p"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])

        

      
      elif timeperiod['from_period']!=None and timeperiod['to_period']!=None:
              if timeperiod['from_unit'] == 'quarter' and timeperiod['to_unit'] == 'month':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
                  dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")

                  where_conditions.append(f"a.{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_month_colname'])
                  dimensions.append(sql_colnames['timeperiod_month_colname'])
                  time_period_growth=sql_colnames['timeperiod_month_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")

              if timeperiod['from_unit'] == 'month' and timeperiod['to_unit'] == 'quarter':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_month_colname'])
                  dimensions.append(sql_colnames['timeperiod_month_colname'])
                  time_period_growth=sql_colnames['timeperiod_month_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")

                  where_conditions.append(f"a.{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
                  dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")

              if timeperiod['from_unit'] == 'month' and timeperiod['to_unit'] == 'month':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_month_colname'])
                  dimensions.append(sql_colnames['timeperiod_month_colname'])
                  time_period_growth=sql_colnames['timeperiod_month_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")

                  where_conditions.append(f"a.{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_month_colname'])
                  dimensions.append(sql_colnames['timeperiod_month_colname'])
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")

              if timeperiod['from_unit'] == 'quarter' and timeperiod['to_unit'] == 'quarter':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
                  dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                  time_period_growth=sql_colnames['timeperiod_quarter_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")

                  where_conditions.append(f"a.{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
                  dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")

              if timeperiod['from_unit'] == 'quarter' and timeperiod['to_unit'] == 'year':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
                  dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                  time_period_growth=sql_colnames['timeperiod_quarter_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
    
                  where_conditions.append(f"a.{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")
                  group_by_columns.append(sql_colnames['timeperiod_year_colname'])
                  dimensions.append(sql_colnames['timeperiod_year_colname'])
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")

              if timeperiod['from_unit'] == 'year' and timeperiod['to_unit'] == 'quarter':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
                  group_by_columns.append(sql_colnames['timeperiod_year_colname'])
                  dimensions.append(sql_colnames['timeperiod_year_colname'])
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
    
                  where_conditions.append(f"a.{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
                  dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                  time_period_growth=sql_colnames['timeperiod_quarter_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")

              if timeperiod['from_unit'] == 'year' and timeperiod['to_unit'] == 'year':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
                  group_by_columns.append(sql_colnames['timeperiod_year_colname'])
                  dimensions.append(sql_colnames['timeperiod_year_colname'])
                  time_period_growth=sql_colnames['timeperiod_year_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
    
                  where_conditions.append(f"a.{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")
                  group_by_columns.append(sql_colnames['timeperiod_year_colname'])
                  dimensions.append(sql_colnames['timeperiod_year_colname'])
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")

      else:
                  
          if timeperiod['from_period']!=None:
              if timeperiod['from_unit'] == 'month':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_month_colname'])
                  dimensions.append(sql_colnames['timeperiod_month_colname'])
                  time_period_growth=sql_colnames['timeperiod_month_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
              elif timeperiod['from_unit'] == 'quarter':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
                  dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                  time_period_growth=sql_colnames['timeperiod_quarter_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_quarter_colname']} >= '{timeperiod['from_period']}'")
              elif timeperiod['from_unit'] == 'year':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
                  group_by_columns.append(sql_colnames['timeperiod_year_colname'])
                  dimensions.append(sql_colnames['timeperiod_year_colname'])
                  time_period_growth=sql_colnames['timeperiod_year_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_year_colname']} >= {timeperiod['from_period']}")
          if timeperiod['to_period']!=None:
              if timeperiod['to_unit'] == 'month':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_month_colname'])
                  dimensions.append(sql_colnames['timeperiod_month_colname'])
                  time_period_growth=sql_colnames['timeperiodmonth_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_month_colname']} <= '{timeperiod['to_period']}'")
              elif timeperiod['to_unit'] == 'quarter':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
                  dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                  time_period_growth=sql_colnames['timeperiod_quarter_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_quarter_colname']} <= '{timeperiod['to_period']}'")
              elif timeperiod['to_unit'] == 'year':
                  where_conditions.append(f"a.{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")
                  where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")
                  group_by_columns.append(sql_colnames['timeperiod_year_colname'])
                  dimensions.append(sql_colnames['timeperiod_year_colname'])
                  time_period_growth=sql_colnames['timeperiod_year_colname']
                  denom_where_conditions.append(f"{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")
                  denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_year_colname']} <= {timeperiod['to_period']}")

        #NEW ADDITION FOR EXCLUDED TIME PERIOD
      if timeperiod.get('excluded_periods'):
            excluded_conditions = []
            denom_excluded_conditions = []
            for period in timeperiod['excluded_periods']:
                if "Q" in period or "q" in period:  # Assuming the quarter format includes "Q1", "q2", etc.

                    excluded_conditions = " AND ".join(
                        [f"a.{sql_colnames['timeperiod_quarter_colname']} != '{period}'" for period in timeperiod['excluded_periods']]
                    )
                    
                    denom_excluded_conditions = " AND ".join(
                    [f"{sql_colnames['timeperiod_quarter_colname']} != '{period}'" for period in timeperiod['excluded_periods']]
                    )
                    where_conditions.append(f"({excluded_conditions})")
                    denom_where_conditions.append(f"({denom_excluded_conditions})")
                elif period.isdigit() and len(period)>4: # Check if the period contains only numbers (like a year)
                    excluded_conditions = " AND ".join(
                        [f"a.{sql_colnames['timeperiod_month_colname']} != '{period}'" for period in timeperiod['excluded_periods']]
                    )
                    
                    denom_excluded_conditions = " AND ".join(
                    [f"{sql_colnames['timeperiod_month_colname']} != '{period}'" for period in timeperiod['excluded_periods']]
                    )
                    where_conditions.append(f"({excluded_conditions})")
                    denom_where_conditions.append(f"({denom_excluded_conditions})")
                
                elif period.isdigit(): # Check if the period contains only numbers (like a year)
                    excluded_conditions = " AND ".join(
                        [f"a.{sql_colnames['timeperiod_year_colname']} != '{period}'" for period in timeperiod['excluded_periods']]
                    )
                    
                    denom_excluded_conditions = " AND ".join(
                    [f"{sql_colnames['timeperiod_year_colname']} != '{period}'" for period in timeperiod['excluded_periods']]
                    )
                    where_conditions.append(f"({excluded_conditions})")
                    denom_where_conditions.append(f"({denom_excluded_conditions})")

      if len(where_conditions) == 0:
          where_conditions.append(timeperiod_filters_get["default"])
          denom_where_conditions.append(timeperiod_filters_get["default"])
          where_conditions_top_n.append(timeperiod_filters_get["default"])
          denom_where_conditions_top_n.append(timeperiod_filters_get["default"])
    elif datasource == 'mmit':
        if timeperiod['relative_unit']!=None:
            if not previous_flag:
                where_conditions.append(timeperiod_filters_get["r"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
                if timeperiod['relative_unit'][0] == 'm':
                    time_period_growth = 'month_year'
                    dimensions.append(sql_colnames['timeperiod_month_colname'])
                    group_by_columns.append(sql_colnames['timeperiod_month_colname'])
                elif timeperiod['relative_unit'][0] == 'q':
                    time_period_growth = 'quarter'
                    dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                    group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
                elif timeperiod['relative_unit'][0] == 'y':
                    time_period_growth = 'year'
                    dimensions.append(sql_colnames['timeperiod_year_colname'])
                    group_by_columns.append(sql_colnames['timeperiod_year_colname'])
                denom_where_conditions.append(timeperiod_filters_get["r"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
                where_conditions_top_n.append(timeperiod_filters_get["r"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
                denom_where_conditions_top_n.append(timeperiod_filters_get["r"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
            else :
                where_conditions.append(timeperiod_filters_get["p"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
                if timeperiod['relative_unit'][0] == 'm':
                    time_period_growth = 'month_year'
                    dimensions.append(sql_colnames['timeperiod_month_colname'])
                    group_by_columns.append(sql_colnames['timeperiod_month_colname'])
                elif timeperiod['relative_unit'][0] == 'q':
                    time_period_growth = 'quarter'
                    dimensions.append(sql_colnames['timeperiod_quarter_colname'])
                    group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
                elif timeperiod['relative_unit'][0] == 'y':
                    time_period_growth = 'year'
                    dimensions.append(sql_colnames['timeperiod_year_colname'])
                    group_by_columns.append(sql_colnames['timeperiod_year_colname'])
                denom_where_conditions.append(timeperiod_filters_get["p"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
                where_conditions_top_n.append(timeperiod_filters_get["p" + str(timeperiod['relative_number']) + timeperiod['relative_unit'][0]])
                denom_where_conditions_top_n.append(timeperiod_filters_get["p"+str(timeperiod['relative_number'])+timeperiod['relative_unit'][0]])
        
        if timeperiod['relative_unit']=='month' or timeperiod['from_unit']=='month':
          dimensions.append(sql_colnames['timeperiod_month_colname'])
          group_by_columns.append(sql_colnames['timeperiod_month_colname'])
          where_conditions.append(f"a.{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
          where_conditions_top_n.append(f"a.{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
          time_period_growth=sql_colnames['timeperiod_month_colname']
          denom_where_conditions.append(f"{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
          denom_where_conditions_top_n.append(f"{sql_colnames['timeperiod_month_colname']} >= '{timeperiod['from_period']}'")
        elif timeperiod['relative_unit']=='quarter' or timeperiod['from_unit']=='quarter':
          dimensions.append(sql_colnames['timeperiod_quarter_colname'])
          group_by_columns.append(sql_colnames['timeperiod_quarter_colname'])
          where_conditions.append(f"a.{sql_colnames['month_quarterrank_colname']} = 1")
          where_conditions_top_n.append(f"a.{sql_colnames['month_quarterrank_colname']} = 1")
          time_period_growth=sql_colnames['timeperiod_quarter_colname']
          denom_where_conditions.append(f"{sql_colnames['month_quarterrank_colname']} = 1")
          denom_where_conditions_top_n.append(f"{sql_colnames['month_quarterrank_colname']} = 1")
        elif timeperiod['relative_unit']=='year' or timeperiod['from_unit']=='year':
          dimensions.append(sql_colnames['timeperiod_year_colname'])
          group_by_columns.append(sql_colnames['timeperiod_year_colname'])
          where_conditions.append(f"a.{sql_colnames['month_yearrank_colname']} = 1")
          where_conditions_top_n.append(f"a.{sql_colnames['month_yearrank_colname']} = 1")
          time_period_growth=sql_colnames['timeperiod_year_colname']
          denom_where_conditions.append(f"{sql_colnames['month_yearrank_colname']} = 1")
          denom_where_conditions_top_n.append(f"{sql_colnames['month_yearrank_colname']} = 1")
        if len(where_conditions) == 0:
          where_conditions.append(timeperiod_filters_get["default"])
          denom_where_conditions.append(timeperiod_filters_get["default"])
    
    if datasource.lower() == 'npa':
        market_colname = sql_colnames['market_colname']
        #market_filter = f("regexp like (upper({market_colname}), {market_name})")
        for m in market_name :
            where_conditions.append(f"regexp_like (lower({market_colname}), '{m}')")
            denom_where_conditions.append(f"regexp_like (lower({market_colname}), '{m}')")
            where_conditions_top_n.append(f"regexp_like (lower({market_colname}), '{m}')")
            denom_where_conditions_top_n.append(f"regexp_like (lower({market_colname}), '{m}')")
    # Add universal filters to main query
    if universal_filters:
        # Prefix column names with 'a.' in universal_filters
        main_universal_filters_prefixed = prefix_columns(universal_filters, sql_colnames, alias='a.')
        where_conditions.append(main_universal_filters_prefixed)
        where_conditions_top_n.append(main_universal_filters_prefixed)
    
    # Remove duplicates by converting to list while preserving order
    dimensions = dedupe(dimensions)
    dimensions_top_n = dedupe(dimensions_top_n)
    growth_partition=dedupe(growth_partition)
    group_by_columns = dedupe(group_by_columns)
    group_by_columns_top_n = dedupe(group_by_columns_top_n)


    print("Dimensions : ",dimensions)
    print("time period growth : ",time_period_growth)
    dimensions_rolling = [dim for dim in dimensions if dim not in {time_period_growth, "month_year", "year", "quarter"}]
    dimensions_top_n = [dim for dim in dimensions_top_n if dim not in {time_period_growth, "month_year", "year", "quarter"}]
    print("Dimensions : ",dimensions)
    print("Dimensions rolling : ",dimensions_rolling)

    if "month_year" in dimensions and (time_period_growth == "year" or time_period_growth == "quarter")  :
        time_period_growth = "month_year"
    elif "quarter" in dimensions and time_period_growth == "year"   :
        time_period_growth = "quarter"

    growth_partition = [item for item in growth_partition if item != "month_year"]
    growth_partition = [item for item in growth_partition if item != "year"]
    growth_partition = [item for item in growth_partition if item != "quarter"]
    where_clause_top_n = ''

    if top_matches:
        where_conditions_top_n=[x.replace('a.a.','a.') for x in where_conditions_top_n]
        
        where_clause_top_n = "\nWHERE\n    " + "\n    AND ".join(where_conditions_top_n)
        
        # Build FROM clause with alias 'a'
        from_clause_top_n = f"\nFROM\n    {datamart_table_name} a"
        select_clause_top_n = "SELECT * FROM ("
        # Initialize SELECT clause with dimensions
        select_clause_top_n += "SELECT\n    " + ",\n    ".join([f"a.{dim}" for dim in dimensions_top_n])


    if channel_names:
        channel_colname = sql_colnames['channel_colname']
        where_conditions.append(f"regexp_like (upper(a.{channel_colname}),{channel_names})")
    if product_names:
        brand_colname = sql_colnames['brand_colname']
        if 'switch' in metrics:
            prev_brand_colname = sql_colnames['prev_brand_colname']
            where_conditions.append(f"((regexp_like (upper(a.{brand_colname}),{product_names})) or (regexp_like (upper(a.{prev_brand_colname}),{product_names})))")
        else:
            where_conditions.append(f"regexp_like (upper(a.{brand_colname}),{product_names})")
    if controller_names:
        controller_colname = sql_colnames['controller_colname']
        where_conditions.append(f"regexp_like (upper(a.{controller_colname}),{controller_names})")
    if pbm_names:
        pbm_colname = sql_colnames['pbm_colname']
        where_conditions.append(f"regexp_like (upper(a.{pbm_colname}),{pbm_names})")
    if rmo_names:
        rmo_colname = sql_colnames['rmo_colname']
        where_conditions.append(f"regexp_like (upper(a.{rmo_colname}),{rmo_names})")
    if benefittype_names:
        benefittype_colname = sql_colnames['benefittype_colname']
        where_conditions.append(f"regexp_like (upper(a.{benefittype_colname}),{benefittype_names})")
    if indication_names:
        indication_colname = sql_colnames['indication_colname']
        where_conditions.append(f"regexp_like (upper(a.{indication_colname}),{indication_names})")
    if grouping_names:
        grouping_colname = sql_colnames[input_master_dict['groupings'][grouping_names[0]][0]]
        where_conditions.append(f"regexp_like (upper(a.{grouping_colname}),{split_list_to_string([x.upper().strip() for x in input_master_dict['groupings'][grouping_names[0]][1]])})")

     #NEW ADDITION FOR EXCLUDING
    if excluded_channel_names:
        channel_colname = sql_colnames['channel_colname']
        where_conditions.append(f"NOT regexp_like (upper(a.{channel_colname}),{excluded_channel_names})")
    
    if excluded_product_names:
        brand_colname = sql_colnames['brand_colname']
        where_conditions.append(f"NOT regexp_like (upper(a.{brand_colname}),{excluded_product_names})")

    if excluded_controller_names:
        controller_colname = sql_colnames['controller_colname']
        where_conditions.append(f"NOT regexp_like (upper(a.{controller_colname}),{excluded_controller_names})")

    if excluded_pbm_names:
        pbm_colname = sql_colnames['pbm_colname']
        where_conditions.append(f"NOT regexp_like (upper(a.{pbm_colname}),{excluded_pbm_names})")

    if excluded_rmo_names:
        rmo_colname = sql_colnames['rmo_colname']
        where_conditions.append(f"NOT regexp_like (upper(a.{rmo_colname}),{excluded_rmo_names})")

    if excluded_benefittype_names:
        benefittype_colname = sql_colnames['benefittype_colname']
        where_conditions.append(f"NOT regexp_like (upper(a.{benefittype_colname}),{excluded_benefittype_names})")

    if excluded_indication_names:
        indication_colname = sql_colnames['indication_colname']
        where_conditions.append(f"NOT regexp_like (upper(a.{indication_colname}),{excluded_indication_names})")
    
    if excluded_grouping_names:
        grouping_colname = sql_colnames[input_master_dict['groupings'][grouping_names[0]][0]]
        where_conditions.append(f"NOT regexp_like (upper(a.{grouping_colname}),{split_list_to_string([x.upper().strip() for x in input_master_dict['groupings'][grouping_names[0]][1]])})")
    



    # Note: **Issue 1 Fix:** Removed metric-type specific filters from the main WHERE clause
    # Instead, apply them within metric expressions
    where_conditions=[x.replace('a.a.','a.') for x in where_conditions]


    where_clause = "\nWHERE\n    " + "\n    AND ".join(where_conditions)
    
    # Build FROM clause with alias 'a'
    from_clause = f"\nFROM\n    {datamart_table_name} a"

    # Initialize SELECT clause with dimensions
    select_clause = "SELECT\n    " + ",\n    ".join([f"a.{dim}" for dim in dimensions])
    growth_partition_clause=" , ".join([f"a.{a2}" for a2 in growth_partition])
    denom_group_by_cols = []




    ##NEW ADDITION For Growth CALCULATION
    if lowest_granularity == 'month_year':
        growth_join_query = f"SELECT DISTINCT {lowest_granularity} FROM {datamart_table_name} WHERE ({highest_granularity}_rank = 1 OR {highest_granularity}_rank >= 3)  or ({highest_granularity}_rank = 2 AND {lowest_granularity}rank >= {max_n} - (SELECT MAX({lowest_granularity}rank) FROM `gco-application-dev`.bai_bcbu_user_adhoc.Spk_enbrel_iqvia_datamart_interim2 WHERE {highest_granularity}_rank = 1))"
    elif lowest_granularity == 'transaction_timestamp':
        growth_join_query = f"SELECT DISTINCT {lowest_granularity} FROM {datamart_table_name} WHERE ({highest_granularity}_rank = 1 OR {highest_granularity}_rank >= 3)  or ({highest_granularity}_rank = 2 AND week_yearrank >= {max_n} - (SELECT MAX(week_yearrank) FROM `gco-application-dev`.bai_bcbu_user_adhoc.Spk_enbrel_iqvia_datamart_interim2 WHERE {highest_granularity}_rank = 1))"

    #NEW ADDITOIN FOR GROWTH CALCULATION
    join_conditions_5 = f"b.{lowest_granularity} = c.{lowest_granularity}"
    join_conditions_6 = f"a.{lowest_granularity} = d.{lowest_granularity}"


    # Initialize lists for metric expressions and joins
    metric_expressions = []
    metric_expressions_top_n = []
    metric_expressions_rolling = []
    joins = []
    joins_top_n = []

    # Iterate over all combinations of metrics and types
    for m_name in metrics:
        metric_rules_dict = metric_businessrules[m_name]
        def get_key(dict, value):
            for key, val in dict.items():
        
                if key == value:
                    return dict[key]
            return dict['all']  #
        
        metric_rules = get_key(metric_rules_dict, market_name)
        if not metric_rules:
            raise ValueError(f"Metric '{m_name}' is not defined in the metric_businessrules.")

        # Extract metric parameters
        ratio = metric_rules.get('ratio', 'N')
        numerator_col_key = metric_rules['numerator_column']
        numerator_col = sql_colnames.get(numerator_col_key, numerator_col_key)
        numerator_filter = metric_rules.get('numerator_filter', '')
        
        def replace_sql_names(filter):
            filter_updated = ''
            for word in filter.split():
                if len(word)> 8:
                    if word[-8:] == '_colname':
                        filter_updated = filter_updated + ' ' + sql_colnames[word]
                    else:
                        filter_updated = filter_updated + ' ' + word
                else:
                    filter_updated = filter_updated + ' ' + word
            return filter_updated

        # print(numerator_filter)
        numerator_filter = replace_sql_names(metric_rules.get('numerator_filter', ''))

        # print(numerator_filter)
        denominator_filter = replace_sql_names(metric_rules.get('denominator_filter', ''))
        where_clause = replace_sql_names(where_clause)
        if where_clause_top_n:
            where_clause_top_n = replace_sql_names(where_clause_top_n)
        
        numerator_aggregate = metric_rules['numerator_aggregate'].upper()
        numerator_distinct = metric_rules.get('numerator_distinct', 'N')
        denominator_aggregate = metric_rules.get('denominator_aggregate', 'SUM').upper()
        denominator_distinct = metric_rules.get('denominator_distinct', 'N')
        metric_alias = metric_rules['alias']
        numerator_metric_alias = metric_rules['numerator_alias']
        denominator_metric_alias = metric_rules['denominator_alias']
        denominator_group_by_exclude_keys = metric_rules.get('denominator_group_by_exclude', [])
        
        if isinstance(denominator_group_by_exclude_keys, str):
            denominator_group_by_exclude_keys = [denominator_group_by_exclude_keys]
        denominator_group_by_exclude_cols = [sql_colnames.get(key, key) for key in denominator_group_by_exclude_keys]
        additional_group_by_keys = metric_rules.get('additional_group_by', [])
        additional_group_by_cols = [sql_colnames.get(key, key) for key in additional_group_by_keys]

        # Build numerator expression with numerator_filter
        numerator_expr = f"a.{numerator_col}"
        numerator_expr2 = f"a.{numerator_col}"


        if numerator_filter:
            if numerator_aggregate.upper() == "COUNT":
                numerator_expr = f"CASE WHEN {numerator_filter} THEN a.{numerator_col} ELSE null END"

                if yoy_flag:
                    numerator_expr2 = f"CASE WHEN {numerator_filter} AND d.month_year IS NOT NULL THEN a.{numerator_col} ELSE null END" #NEW ADDITION FOR GROWTH

            else:
                numerator_expr = f"CASE WHEN {numerator_filter} THEN a.{numerator_col} ELSE 0 END"

                if yoy_flag:
                    numerator_expr2 = f"CASE WHEN {numerator_filter} AND d.month_year IS NOT NULL THEN a.{numerator_col} ELSE 0 END" #NEW ADDITION FOR GROWTH
        if numerator_distinct.upper() == 'Y':
            numerator_agg_expr = f"{numerator_aggregate}(DISTINCT {numerator_expr})"

            if yoy_flag:
                numerator_agg_expr2 = f"{numerator_aggregate}(DISTINCT {numerator_expr2})" #NEW ADDITION FOR GROWTH

        else:
            numerator_agg_expr = f"{numerator_aggregate}({numerator_expr})"

            if yoy_flag:
                numerator_agg_expr2 = f"{numerator_aggregate}({numerator_expr2})" #NEW ADDITION FOR GROWTH


        if ratio.upper() == 'Y':
            # For ratio metrics, build denominator subquery
            denominator_col_key = metric_rules['denominator_column']
            denominator_col = sql_colnames.get(denominator_col_key, denominator_col_key)

                # Build denominator expression with denominator_filter
            denominator_expr = f"{denominator_col}"
            if denominator_filter:
                if not yoy_flag:
                    if denominator_aggregate.upper() == "COUNT":
                        denominator_expr = f"CASE WHEN {denominator_filter} THEN {denominator_col} ELSE null END"
                    else:
                        denominator_expr = f"CASE WHEN {denominator_filter} THEN {denominator_col} ELSE 0 END"
                    
                else :
                    if denominator_aggregate.upper() == "COUNT":
                        denominator_expr = f"CASE WHEN {denominator_filter} THEN {denominator_col} ELSE null END"
                        denominator_expr2 = f"CASE WHEN {denominator_filter} AND c.month_year IS NOT NULL THEN {denominator_col} ELSE null END" #NEW ADDITION FOR GROWTH
                    else:
                        denominator_expr = f"CASE WHEN {denominator_filter} THEN {denominator_col} ELSE 0 END"
                        denominator_expr2 = f"CASE WHEN {denominator_filter} AND c.month_year IS NOT NULL THEN {denominator_col} ELSE 0 END" #nEW ADDITION FOR GROWTH


                    


            # Apply aggregation function and DISTINCT if specified
            if not yoy_flag:
                if denominator_distinct.upper() == 'Y':
                    denominator_agg_expr = f"{denominator_aggregate}(DISTINCT {denominator_expr})"
                else:
                    denominator_agg_expr = f"{denominator_aggregate}({denominator_expr})"
            else:
                if denominator_distinct.upper() == 'Y':
                    denominator_agg_expr = f"{denominator_aggregate}(DISTINCT {denominator_expr})"
                    denominator_agg_expr2 = f"{denominator_aggregate}(DISTINCT {denominator_expr2})"  #NEW ADDITION FOR GROWTH
                else:
                    denominator_agg_expr = f"{denominator_aggregate}({denominator_expr})"
                    denominator_agg_expr2 = f"{denominator_aggregate}({denominator_expr2})"         #NEW ADDITION FOR GROWTH

            # Define group by columns for denominator (exclude columns in denominator_group_by_exclude)
           
            denom_group_by_cols = [col for col in group_by_columns if col not in denominator_group_by_exclude_cols]
           
            # Build WHERE conditions for denominator subquery (exclude product/channel filter as needed)

            if universal_filters:
                denom_where_conditions.append(universal_filters)  # No 'a.' prefix
            # Apply filters excluding those in denominator_group_by_exclude
            # For example, if 'channel_colname' is excluded, do not apply channel filters in denominator, for loop
            if channel_names and 'channel_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"regexp_like (upper({channel_colname}),{channel_names})")
            if product_names and 'brand_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"regexp_like (upper({brand_colname}),{product_names})")
            if controller_names and 'controller_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"regexp_like (upper({controller_colname}),{controller_names})")
            if pbm_names and 'pbm_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"regexp_like (upper({pbm_colname}),{pbm_names})")
            if rmo_names and 'rmo_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"regexp_like (upper({rmo_colname}),{rmo_names})")
            if indication_names and 'indication_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"regexp_like (upper({indication_colname}),{indication_names})")
            if benefittype_names and 'benefittype_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"regexp_like (upper({benefittype_colname}),{benefittype_names})")
            if grouping_names:
                grouping_colname = sql_colnames[input_master_dict['groupings'][grouping_names[0]][0]]

                denom_where_conditions.append(f"regexp_like (upper({grouping_colname}),{split_list_to_string([x.upper().strip() for x in input_master_dict['groupings'][grouping_names[0]][1]])})")
            
            #NEW ADDITION
            if excluded_channel_names and 'channel_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"NOT regexp_like (upper({channel_colname}),{excluded_channel_names})")
            if excluded_product_names and 'brand_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"NOT regexp_like (upper({brand_colname}),{excluded_product_names})")
            if excluded_controller_names and 'controller_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"NOT regexp_like (upper({controller_colname}),{excluded_controller_names})")
            if excluded_pbm_names and 'pbm_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"NOT regexp_like (upper({pbm_colname}),{excluded_pbm_names})")
            if excluded_rmo_names and 'rmo_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"NOT regexp_like (upper({rmo_colname}),{excluded_rmo_names})")
            if excluded_indication_names and 'indication_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"NOT regexp_like (upper({indication_colname}),{excluded_indication_names})")
            if excluded_benefittype_names and 'benefittype_colname' not in denominator_group_by_exclude_keys:
                denom_where_conditions.append(f"NOT regexp_like (upper({benefittype_colname}),{excluded_benefittype_names})")
            if excluded_grouping_names:
                grouping_colname = sql_colnames[input_master_dict['groupings'][excluded_grouping_names[0]][0]]
                denom_where_conditions.append(f"NOT regexp_like (upper({grouping_colname}),{split_list_to_string([x.upper().strip() for x in input_master_dict['groupings'][excluded_grouping_names[0]][1]])})")


            if denominator_filter:
                denom_where_conditions.append(denominator_filter)

            # Remove any empty strings from where conditions
            denom_where_conditions = [cond for cond in denom_where_conditions if cond]

            denom_where_clause = "\nWHERE\n    " + "\n    AND ".join(denom_where_conditions)
            denom_where_clause = replace_sql_names(denom_where_clause)
            # Build denominator subquery with appropriate aggregation
            denom_group_by_cols_str = ",\n    ".join(denom_group_by_cols)
            print("checking da",denom_group_by_cols_str)
            #print("MY CHECKING : ",denom_group_by_cols_str)
            denom_group_by_cols_str_query = ''
            if denom_group_by_cols_str != '':
                denom_group_by_cols_str_query = f"GROUP BY {denom_group_by_cols_str}"
                if len(denom_group_by_cols)>=1:
                    denom_group_by_cols_str = f"{denom_group_by_cols_str},"
                else :
                    denom_group_by_cols_str = f"{denom_group_by_cols_str}"
            
            if top_matches:


                denominator_subquery_top_n = f"""

        (SELECT
            {denom_group_by_cols_str}
            {denominator_agg_expr} AS total_{denominator_metric_alias}
            FROM
            {datamart_table_name} 
            {denom_where_clause}
            {denom_group_by_cols_str_query}
        ) denom_{metric_alias}
        """
                if yoy_flag :
                    if time_period_growth == 'quarter' or time_period_growth == 'year':
                    #print("Coming here for denom subquery")
                        denominator_subquery = f"""

                (SELECT
                    {denom_group_by_cols_str}
                    {denominator_agg_expr} AS total_{denominator_metric_alias},
                    {denominator_agg_expr2} AS total_{denominator_metric_alias}_partial
                    FROM
                    {datamart_table_name} b
                    LEFT JOIN ({growth_join_query}) c on {join_conditions_5}
                    {denom_where_clause}
                    {denom_group_by_cols_str_query}
                ) denom_{metric_alias}
                """
                    if time_period_growth == 'month_year':
                        denominator_subquery = f"""

                (SELECT
                    {denom_group_by_cols_str}
                    {denominator_agg_expr} AS total_{denominator_metric_alias}
                    FROM
                    {datamart_table_name} 
                    {denom_where_clause}
                    {denom_group_by_cols_str_query}
                ) denom_{metric_alias}
                """

                else :
                    denominator_subquery = f"""

        (SELECT
            {denom_group_by_cols_str}
            {denominator_agg_expr} AS total_{denominator_metric_alias}
            FROM
            {datamart_table_name} 
            {denom_where_clause}
            {denom_group_by_cols_str_query}
        ) denom_{metric_alias}
        """

            

            if not yoy_flag :
                denominator_subquery = f"""

        (SELECT
            {denom_group_by_cols_str}
            {denominator_agg_expr} AS total_{denominator_metric_alias}
            FROM
            {datamart_table_name} 
            {denom_where_clause}
            {denom_group_by_cols_str_query}
        ) denom_{metric_alias}
        """
            elif yoy_flag: #NEW ADDITION FOR GROWTH
                if time_period_growth == 'quarter' or time_period_growth == 'year':
                    #print("Coming here for denom subquery")
                    denominator_subquery = f"""

            (SELECT
                {denom_group_by_cols_str}
                {denominator_agg_expr} AS total_{denominator_metric_alias},
                {denominator_agg_expr2} AS total_{denominator_metric_alias}_partial
                FROM
                {datamart_table_name} b
                LEFT JOIN ({growth_join_query}) c on {join_conditions_5}
                {denom_where_clause}
                {denom_group_by_cols_str_query}
            ) denom_{metric_alias}
            """
                if time_period_growth == 'month_year':
                    denominator_subquery = f"""

            (SELECT
                {denom_group_by_cols_str}
                {denominator_agg_expr} AS total_{denominator_metric_alias}
                FROM
                {datamart_table_name} 
                {denom_where_clause}
                {denom_group_by_cols_str_query}
            ) denom_{metric_alias}
            """

            # Build join condition based on denom_group_by_cols
            join_conditions = ' AND '.join([f"a.{col} = denom_{metric_alias}.{col}" for col in denom_group_by_cols])
            if top_matches:
                join_conditions_top_n = ' AND '.join([f"a.{col} = denom_{metric_alias}.{col}" for col in denom_group_by_cols])
            # Add denominator subquery to joins
            if denom_group_by_cols_str !='':
                joins.append(f"LEFT JOIN {denominator_subquery} ON {join_conditions}")
                if top_matches:
                    joins_top_n.append(f"LEFT JOIN {denominator_subquery_top_n} ON {join_conditions_top_n}")
   

            else:
                joins.append(f"CROSS JOIN {denominator_subquery}")
                if top_matches:
                    joins_top_n.append(f"CROSS JOIN {denominator_subquery_top_n}")
            

            


            # Build metric expressions with handling for NULL denominators
            # **Issue 2 Fix:** Replace NULL denominators with 0 using COALESCE
            metric_expressions.append(f"{numerator_agg_expr} AS {numerator_metric_alias}")
            metric_expressions.append(f"COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0) AS {denominator_metric_alias}")
            metric_expressions.append(f"{numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0) AS {metric_alias}")
            if rolling_matches:
                print("\nCOMING HERE TO ROLLING, this is : ",numerator_metric_alias)
                if not cut_by_rolling:
                    metric_expressions.append(f"CEIL((ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY a.{time_period_growth} ASC))/ {rolling_value}) AS segment")
                else:
                    metric_expressions.append(f"CEIL((ROW_NUMBER() OVER (PARTITION BY {cut_by_rolling_code} ORDER BY a.{time_period_growth} ASC))/ {rolling_value}) AS segment")



                metric_expressions_rolling.append(f"SUM({numerator_metric_alias}) AS rolled_{numerator_metric_alias}")
                
                metric_expressions_rolling.append(f"SUM({denominator_metric_alias}) AS rolled_{denominator_metric_alias}")
                metric_expressions_rolling.append(f"SUM({numerator_metric_alias}) / SUM({denominator_metric_alias}) AS rolled_{metric_alias}")
                
                #metric_expressions.append(f"CEIL((ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY a.month_year ASC))/ {rolling_value}) AS segment")

                 



            if yoy_flag: #NEW ADDITION FOR GROWTH
                if time_period_growth == 'month_year':
                    metric_expressions.append(f"{numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0) - LAG({numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0),12) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})" f" AS YOY_Growth")
                if time_period_growth == 'year':
                    metric_expressions.append(f"{numerator_agg_expr2} AS {numerator_metric_alias}_partial")
                    metric_expressions.append(f"{numerator_agg_expr2}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}_partial), 0), 0) AS {metric_alias}_partial")





                    metric_expressions.append(f"""
                                              
            CASE 
            WHEN LAG((a.{time_period_growth}_rank),1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) - a.{time_period_growth}_rank = 1 AND
                LAG({numerator_agg_expr2}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}_partial), 0), 0),1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) <>
                 LAG({numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0),1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})

                THEN {numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0)
                    - LAG({numerator_agg_expr2}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}_partial), 0), 0),1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})
            
            WHEN LAG((a.{time_period_growth}_rank),1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) - a.{time_period_growth}_rank = 1 AND
                LAG({numerator_agg_expr2}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}_partial), 0), 0),1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) =
                 LAG({numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0),1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})

                THEN {numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0) - 
                 LAG({numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0),1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})
            
            ELSE NULL 
            END AS YOY_growth

        """)

                if time_period_growth == 'quarter':
                    metric_expressions.append(f"{numerator_agg_expr2} AS {numerator_metric_alias}_partial")
                    metric_expressions.append(f"{numerator_agg_expr2}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}_partial), 0), 0) AS {metric_alias}_partial")





                    metric_expressions.append(f"""
                                              
            CASE 
            WHEN LAG((a.{time_period_growth}_rank),4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) - a.{time_period_growth}_rank = 4 AND
                LAG({numerator_agg_expr2}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}_partial), 0), 0),4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) <>
                 LAG({numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0),4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})

                THEN {numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0)
                    - LAG({numerator_agg_expr2}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}_partial), 0), 0),4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})
            
            WHEN LAG((a.{time_period_growth}_rank),4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) - a.{time_period_growth}_rank = 4 AND
                LAG({numerator_agg_expr2}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}_partial), 0), 0),4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) =
                 LAG({numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0),4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})

                THEN {numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0) - 
                 LAG({numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0),4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})
            
            ELSE NULL 
            END AS YOY_growth 

        """)

            if top_matches:
                metric_expressions_top_n.append(f"{numerator_agg_expr} AS {numerator_metric_alias}")
                metric_expressions_top_n.append(f"COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0) AS {denominator_metric_alias}")
                metric_expressions_top_n.append(f"{numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0) AS {metric_alias}")
                
                if cut_by_for_partition :
                    
                    metric_expressions_top_n.append(f"ROW_NUMBER() OVER (PARTITION BY {partition_rank} ORDER BY COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0) DESC) AS ranki")
                else:
                    metric_expressions_top_n.append(f"ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0) DESC) AS ranki")
                                          
        

                
                
                                                 








        else:
            if top_matches:
            # For non-ratio metrics like volume
                metric_expressions.append(f"{numerator_agg_expr} AS {numerator_metric_alias}")
            #New Code

                metric_expressions_top_n.append(f"{numerator_agg_expr} AS {numerator_metric_alias}")
                if time_period_growth:
                    if len(cut_by)>1:
                        metric_expressions.append(f"{numerator_agg_expr}/sum({numerator_agg_expr}) OVER (Partition BY {contribution_rank},a.{time_period_growth}) AS Contribution")

                    else:
                        metric_expressions.append(f"{numerator_agg_expr}/sum({numerator_agg_expr}) OVER (Partition BY a.{time_period_growth}) AS Contribution")
                else :
                    metric_expressions.append(f"{numerator_agg_expr}/sum({numerator_agg_expr}) OVER (Partition BY 1) AS Contribution")



                #metric_expressions_top_n.append(f"{numerator_agg_expr}*1.0 / NULLIF(COALESCE(MAX(denom_{metric_alias}.total_{denominator_metric_alias}), 0), 0) AS {metric_alias}")
                if cut_by_for_partition :
                    metric_expressions_top_n.append(f"ROW_NUMBER() OVER (PARTITION BY {partition_rank} ORDER BY {numerator_agg_expr}*1.0 DESC) AS ranki")
                else:

                    metric_expressions_top_n.append(f"ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY {numerator_agg_expr}*1.0 DESC) AS ranki")
            else:
                metric_expressions.append(f"{numerator_agg_expr} AS {numerator_metric_alias}")
                if time_period_growth:
                    if len(cut_by)>1:
                        metric_expressions.append(f"{numerator_agg_expr}/sum({numerator_agg_expr}) OVER (Partition BY {contribution_rank},a.{time_period_growth}) AS Contribution")

                    else:
                        metric_expressions.append(f"{numerator_agg_expr}/sum({numerator_agg_expr}) OVER (Partition BY a.{time_period_growth}) AS Contribution")
                else :
                    metric_expressions.append(f"{numerator_agg_expr}/sum({numerator_agg_expr}) OVER (Partition BY 1) AS Contribution")




            if yoy_flag: #NEW ADDITION FOR GROWTH
                if time_period_growth == 'month_year':
                    metric_expressions.append(f"{numerator_agg_expr}*1.0  - LAG({numerator_agg_expr}*1.0 ,12) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})" f" AS YOY_Growth")
                if time_period_growth =='year':
                    metric_expressions.append(f"{numerator_agg_expr2} AS {numerator_metric_alias}_partial")


                    metric_expressions.append(f"""
                                              
            CASE 
            WHEN LAG((a.{time_period_growth}_rank),1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) - a.{time_period_growth}_rank = 1 AND
                LAG({numerator_agg_expr2}*1.0 ,1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) <>
                 LAG({numerator_agg_expr}*1.0 ,1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})

                THEN {numerator_agg_expr}*1.0 
                    - LAG({numerator_agg_expr2}*1.0,1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})
            
            WHEN LAG((a.{time_period_growth}_rank),1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) - a.{time_period_growth}_rank = 1 AND
                LAG({numerator_agg_expr2}*1.0 ,1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) =
                 LAG({numerator_agg_expr}*1.0 ,1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})

                THEN {numerator_agg_expr}*1.0  - 
                 LAG({numerator_agg_expr}*1.0 ,1) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})
            
            ELSE NULL 
            END AS YOY_growth

        """)

                if time_period_growth == 'quarter':
                    metric_expressions.append(f"{numerator_agg_expr2} AS {numerator_metric_alias}_partial")
                    metric_expressions.append(f"""
                                              
            CASE 
            WHEN LAG((a.{time_period_growth}_rank),4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) - a.{time_period_growth}_rank = 4 AND
                LAG({numerator_agg_expr2}*1.0,4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) <>
                 LAG({numerator_agg_expr}*1.0,4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})

                THEN {numerator_agg_expr}*1.0 
                    - LAG({numerator_agg_expr2}*1.0,4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})
            
            WHEN LAG((a.{time_period_growth}_rank),4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) - a.{time_period_growth}_rank = 4 AND
                LAG({numerator_agg_expr2}*1.0 ,4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth}) =
                 LAG({numerator_agg_expr}*1.0 ,4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})

                THEN {numerator_agg_expr}*1.0  - 
                 LAG({numerator_agg_expr}*1.0 ,4) OVER (PARTITION BY {growth_partition_clause} ORDER BY a.{time_period_growth})
            
            ELSE NULL 
            END AS YOY_growth 

        """)
                

            if filtered_matches_quarter:
                if time_period_growth == 'year':
                    #metric_expressions_top_n.append(f"{numerator_agg_expr} AS {numerator_metric_alias}")
                    metric_expressions.append(f"""
                                              
            CASE 
            WHEN COUNT(DISTINCT a.quarter)<>4 
            THEN 
            {numerator_agg_expr}/(COUNT(DISTINCT a.month_year)/3)

            ELSE 
            {numerator_agg_expr}/COUNT(DISTINCT a.quarter)
             
            END AS {numerator_metric_alias}_quarterly_average 

        """)
                                                           

            if filtered_matches_month:
                
                #metric_expressions.append(f"{numerator_agg_expr} AS {numerator_metric_alias}")
                if time_period_growth == 'year':
                    metric_expressions.append(f"{numerator_agg_expr}/COUNT(DISTINCT a.month_year) AS {numerator_metric_alias}_monthly_average")
                elif time_period_growth == 'quarter':
                    metric_expressions.append(f"{numerator_agg_expr}/COUNT(DISTINCT a.month_year) AS {numerator_metric_alias}_monthly_average")





                
            if rolling_matches:
                if not cut_by:
                    print("Coming here without cut_by")
                    
               
                    metric_expressions.append(f"CEIL((ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY a.{time_period_growth} ASC))/ {rolling_value}) AS segment")
                    
                      #  metric_expressions.append(f"CEIL((ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY a.month_year ASC))/ {rolling_value}) AS segment")
                else:
                    print("Coming here with cut_by")
                    #if time_period_growth == 'quarter':
               
                    metric_expressions.append(f"CEIL((ROW_NUMBER() OVER (PARTITION BY {cut_by_rolling_code} ORDER BY a.{time_period_growth} ASC))/ {rolling_value}) AS segment")
                    #if time_period_growth == 'month_year':
                        #metric_expressions.append(f"CEIL((ROW_NUMBER() OVER (PARTITION BY {cut_by_rolling_code} ORDER BY a.month_year ASC))/ {rolling_value}) AS segment")






                metric_expressions_rolling.append(f"SUM({numerator_metric_alias}) AS rolled_{numerator_metric_alias}")
            
    top_n_query=None
    if top_matches:
        select_clause_top_n += ",\n    " + ",\n    ".join(metric_expressions_top_n)
        print("\n\nGroup by columns : ",group_by_columns)
        primary_order_by_metric = denominator_metric_alias if denominator_metric_alias else numerator_metric_alias

        # Build GROUP BY clause
        group_by_columns_with_a_top_n = [f"a.{col}" for col in group_by_columns_top_n]
        group_by_clause_top_n = "\nGROUP BY\n    " + ",\n    ".join(group_by_columns_with_a_top_n)
        if top_matches:
            order_by_clause_top_n = f"\nORDER BY\n    {primary_order_by_metric} DESC"
            order_by_clause_top_n += f",\n    " + ",\n    ".join(group_by_columns_with_a_top_n)
        else:
            order_by_clause_top_n = "\nORDER BY\n    " + ",\n    ".join(group_by_columns_with_a_top_n)
        # Build ORDER BY clause
        # Add ORDER BY for testing top_n
        #order_by_clause = f"\nORDER BY\n    {metric_alias} DESC"
        # Now add the ORDER BY for group_by_columns_with_a on the next line
        #order_by_clause += f",\n    " + ",\n    ".join(group_by_columns_with_a)
        if top_matches:
            top_value = int(top_matches[0])  # Get the first match and convert to int
            
            order_by_clause_top_n += f"\n)where ranki<={top_value}"
        top_n_query = select_clause_top_n + from_clause_top_n
        if joins_top_n:
            top_n_query += "\n" + "\n".join(joins_top_n)
        top_n_query +=where_clause_top_n + group_by_clause_top_n + order_by_clause_top_n;



    # Combine SELECT clause
    select_clause += ",\n    " + ",\n    ".join(metric_expressions)

    # Build GROUP BY clause
    group_by_columns_with_a = [f"a.{col}" for col in group_by_columns]
    group_by_clause = "\nGROUP BY\n    " + ",\n    ".join(group_by_columns_with_a)

    # Build ORDER BY clause
    order_by_clause = "\nORDER BY\n    " + ",\n    ".join(group_by_columns_with_a)

    # Combine all parts
    query = select_clause + from_clause

    # Add joins if any
    if joins:
        query += "\n" + "\n".join(joins)
    
   

    # Add where clause
    joins1=[]
    join_columns_2 = []

    joins2=[]

    if yoy_flag : #NEW ADDITION FOR GROWTH
        if time_period_growth == 'quarter' or time_period_growth == 'year':
        

            joins2.append(f"LEFT  JOIN ({growth_join_query}) d ON {join_conditions_6}")
            if joins2:
                query += "\n" + "\n".join(joins2)
            if time_period_growth == 'quarter':
                group_by_clause+= ",\n a.quarter_rank"
                order_by_clause+= ",\n a.quarter_rank"
            if time_period_growth == 'year':
                group_by_clause+= ",\n a.year_rank"
                order_by_clause+= ",\n a.year_rank"



    
    if top_n_query :
            subquery = f"({top_n_query}) subquery_1"
            # Dynamically generate the join condition
            join_columns1 = set(group_by_columns_top_n)  # Use the group_by_columns from the main query as join columns
            # If the subquery has the same columns to join on, use them for the join condition
            # Assuming `subquery_1` has the same `mmit_controller_name` (or you can extract this from subquery dynamically)
            if denom_group_by_cols:
                join_columns_2 = set(denom_group_by_cols).intersection(join_columns1)
            else:
                join_columns_2 = set(group_by_columns_top_n).intersection(join_columns1)
            if denom_group_by_cols:
                join_conditions1 = ' AND '.join([f"denom_{metric_alias}.{col} = subquery_1.{col}" for col in join_columns_2])
            else:
                join_conditions1 = ' AND '.join([f"a.{col} = subquery_1.{col}" for col in join_columns_2])

            # Add the dynamic join condition to the joins list

            joins1.append(f"INNER JOIN {subquery} ON {join_conditions1}")
            #join_columns_2 = set(denom_group_by_cols).intersection(join_columns1)
            if joins1 :
                     query += "\n" + "\n".join(joins1)  # Add any JOINs


    
    query += where_clause
    # Add group by and order by clauses

    query += group_by_clause + order_by_clause

    if rolling_matches:
        select_clause_rolling = "SELECT\n    " + ",\n    ".join([f"{dim}" for dim in dimensions_rolling])

    if rolling_matches :
      select_clause_rolling +=  "\n ,   " + ",\n    ".join(metric_expressions_rolling)
      select_clause_rolling+=",segment\n"
      select_clause_rolling+=" FROM "
      group_by_rolling = [f"{col}" for col in dimensions_rolling]
      group_by_clause_rolling = "\nGROUP BY segment,\n    " + ",\n    ".join([f"{dim}" for dim in dimensions_rolling])
      
      query = f"{select_clause_rolling}({query}) t {group_by_clause_rolling};"

    else:
        query += ";"


    # print("Generated SQL Query:")
    # print(query)
    return query





# Ensure input_master_dict is defined before calling get_res

def get_res(input_text: str, input_master_dict=master_dict) -> dict:
    # Use input_text directly instead of text
    input_request = fn_primary_function(
      text,
      metrics_dict, brand_dict, custom_dict,
                       timeperiod_dict, channel_dict, indication_dict, controller_dict, pbm_dict,rmo_dict, benefittype_dict,
                       formulary_dict, market_dict, market_products_dict, operator_dict, visualization_dict, splitby_dict, grouping_dict,datasource_dict,entity_fuzzymatch_threshold_lst
  )
    
    print("\nInput Request: ")
    print(input_request)
    
    (metrics, market_name, product_names, start_date, end_date, metric_frequency,channel_names, controller_names, pbm_names, rmo_names, benefittype_names, indication_names, grouping_names,cut_by, datasource, datamart_table_name,timeperiod,excluded_metrics, excluded_market_name, excluded_product_names,excluded_channel_names, excluded_controller_names,
            excluded_pbm_names, excluded_rmo_names, excluded_benefittype_names, excluded_indication_names, excluded_grouping_names,
            excluded_cut_by, excluded_datasource,partition_cut_by) = clean_inputs(input_request, input_master_dict=input_master_dict)
    
    print("\partition_cut_by in clean_inputs : ",partition_cut_by)
    print("\nStart Dtae : ")
    print(start_date)
    print("\nEnd Date")
    print(end_date)
    print("\nTime Period")
    print(timeperiod)
    if metric_frequency=='m':
        helper_freq=' (Monthly)'
        
    elif metric_frequency=='w':
        helper_freq=' (Weekly)'
    elif metric_frequency=='y':
        helper_freq=' (Yearly)'

    else:
        helper_freq=''

    metrics_texts=[]
    for mmr in metrics:
        try:
            if market_name in metrics_business_rules_text_dict[datasource].keys():
                metrics_text=metrics_business_rules_text_dict[datasource][market_name][mmr]
            else:
                metrics_text=metrics_business_rules_text_dict[datasource]['all'][mmr]
        except:
            metrics_text=''
            # metrics_text='Metric business rule text undefined. Please define it.'
            pass
        metrics_text=str(mmr).title()+' :'+metrics_text

        metrics_texts.append(metrics_text)

    
        
    if len(metrics)>1:
        helper_0='What are '+(' and '.join(metrics)).title()+'?'
    else:
        helper_0='What is '+(' and '.join(metrics)).title()+'?'

    helper_0+=helper_freq
    # if start_date!='' and end_date!='':
    #   helper_1='For time period between '+str(start_date)+' and '+str(end_date)
    # else:
    #   helper_1=''

    if len(product_names)>0:
        helper_2='For products: '
        helper_2+=product_names.replace('(','').replace(')','').replace('|',' and ')
    else:
        helper_2=''


    if len(channel_names)>0:
        helper_3='For channels:'
        helper_3+=channel_names.replace('(','').replace(')','').replace('|',' and ')
    else:
        helper_3=''

    if len(controller_names)>0:
        helper_4='For Controllers:'
        helper_4+=controller_names.replace('(','').replace(')','').replace('|',' and ')
    else:
        helper_4=''



    if len(pbm_names)>0:
        helper_5='For PBMs:'
        helper_5+=pbm_names.replace('(','').replace(')','').replace('|',' and ')

    else:
        helper_5=''

    if len(rmo_names)>0:
        helper_6='For RMOs:'
        helper_6+=rmo_names.replace('(','').replace(')','').replace('|',' and ')

    else:
        helper_6=''


    if len(benefittype_names)>0:
        helper_7='For Benefit Types:'
        helper_7+=benefittype_names.replace('(','').replace(')','').replace('|',' and ')
    else:
        helper_7=''


    
    if len(indication_names)>0:
        helper_8='For Indications:'
        helper_8+=indication_names.replace('(','').replace(')','').replace('|',' and ')
    else:
        helper_8=''


    if helper_0!='':
        print(helper_0)
    # if start_date!='' and end_date!='':
    #   helper_1='For time period between '+str(start_date)+' and '+str(end_date)
    # else:
    #   helper_1=''

    # if helper_1!='':
    #   print(helper_1)
    
    if helper_2!='':
        print(helper_2.title())
    
    if helper_3!='':
        print(helper_3.title())
    
    if helper_4!='':
        print(helper_4.title())
    
    if helper_5!='':
        print(helper_5.title())
    
    if helper_6!='':
        print(helper_6.title())
    
    if helper_7!='':
        print(helper_7.title())
    
    if helper_8!='':
        print(helper_8.title())


    for mm in metrics_texts:
        print(mm)
    
    if len(cut_by)>0:
        print('Split by: ' + ', '.join(cut_by))
    try:
        date_range = process_text_date(text, reference_date)[0]
        start_date=date_range['start_date']
        end_date=date_range['end_date']
    except:
        pass


    try:
        start_date=int(start_date[:7].replace('-',''))
        end_date=int(end_date[:7].replace('-',''))
    except:
        pass


    # Generate SQL query
    query_to_run = create_sql(
        metrics,
        market_name,
        product_names,
        start_date,
        end_date,
        metric_frequency,
        channel_names,
        controller_names,
        pbm_names,
        rmo_names,
        benefittype_names,
        indication_names,
        grouping_names,
        cut_by,
        datasource,
        datamart_table_name,
        timeperiod,
        input_master_dict=input_master_dict
    ).replace("''", "'")

    # Return the output as a dictionary instead of printing
    return {
        'helper_texts': helper_texts,
        'metric_texts': metrics_texts,
        'sql_query': query_to_run
    }

@app.post("/process_text")
async def process_text(input_data: InputText, x_api_key: str = Header(None), input_master_dict=master_dict):
    # Check if the API key is valid
    if x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized. Invalid API key.")
    
    # Call the get_res function with input_data.text and input_master_dict
    result = get_res(input_data.text, input_master_dict)
    return JSONResponse(content={"result": result})

# To run the app with Uvicorn:
# uvicorn main:app --reload

# COMMAND ----------

  
