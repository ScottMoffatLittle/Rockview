# The ID for invoiceSalesTeam & transactionSalesTeam is werid, and takes the form of 185330_28997
# Might want to grab on the 'transaction' field, and make chunk size smaller?
# create 3rd / 4th dict option, for batch_key / batch range?

ID_AND_DATE_COLS = {
	'account': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
	'transactionLine': {'unique_key': 'uniquekey', 'date_col': 'linelastmodifieddate'},
	'AccountingPeriod': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
	'CUSTOMERCORD_360_COMMISSION_RULE': {'unique_key': 'id', 'date_col': 'lastmodified'},
	'CUSTOMERCORD_360_COMMISSION_TRACKING': {'unique_key': 'id', 'date_col': 'lastmodified'},
	'Quota': {'unique_key': 'id', 'date_col': 'date'},
	'classification': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
	'employee': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
	'entity': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
	'item': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
	'transaction': {'unique_key': 'id', 'date_col': 'lastmodifieddate'},
	'invoiceSalesTeam': {'unique_key': 'id', 'date_col': 'lastmodifieddate', 'batch_key': 'transaction', 'batch_range': 10000},
	'transactionSalesTeam': {'unique_key': 'id', 'date_col': 'lastmodifieddate', 'batch_key': 'transaction', 'batch_range': 10000}
}