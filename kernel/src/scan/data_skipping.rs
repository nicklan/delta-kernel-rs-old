use std::collections::HashSet;
use std::io::BufReader;
use std::sync::Arc;

use arrow_arith::boolean::{is_not_null, not};
use arrow_array::{new_null_array, RecordBatch, StringArray, StructArray};
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;
use arrow_select::filter::filter_record_batch;
use arrow_select::nullif::nullif;
use tracing::debug;

use crate::error::{DeltaResult, Error};
use crate::scan::Expression;
use crate::schema::SchemaRef;

pub(crate) fn data_skipping_filter(
    actions: RecordBatch,
    table_schema: &SchemaRef,
    predicate: &Expression,
) -> DeltaResult<RecordBatch> {
    let adds = actions
        .column_by_name("add")
        .ok_or(Error::MissingColumn("Column 'add' not found.".into()))?
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or(Error::UnexpectedColumnType(
            "Expected type 'StructArray'.".into(),
        ))?;
    let stats = adds
        .column_by_name("stats")
        .ok_or(Error::MissingColumn("Column 'stats' not found.".into()))?
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or(Error::UnexpectedColumnType(
            "Expected type 'StringArray'.".into(),
        ))?;

    // parse each row as json using the stats schema from data skipping filter
    // HACK see https://github.com/apache/arrow/issues/33662
    let field_names: HashSet<_> = predicate.references();

    // Build the stats read schema by extracting the column names referenced by the predicate,
    // extracting the corresponding field from the table schema, and inserting that field.
    let data_fields: Vec<_> = table_schema.fields.iter()
        .filter(|field| field_names.contains(&field.name as &str))
        .filter_map(|field| Field::try_from(field).ok())
        .collect::<Vec<_>>();

    let stats_schema = Schema::new(vec![
        Field::new(
            "minValues",
            DataType::Struct(data_fields.clone().into()),
            true,
        ),
        Field::new("maxValues", DataType::Struct(data_fields.clone().into()), true),
    ]);
    let parsed = concat_batches(
        &stats_schema.clone().into(),
        stats
            .iter()
            .map(|json_string| hack_parse(&data_fields, &stats_schema, json_string))
            .collect::<Result<Vec<_>, _>>()?
            .iter(),
    )?;

    let skipping_vector = predicate.construct_metadata_filters(parsed)?;
    let skipping_vector = &is_not_null(&nullif(&skipping_vector, &not(&skipping_vector)?)?)?;

    let before_count = actions.num_rows();
    let after = filter_record_batch(&actions, skipping_vector)?;
    debug!(
        "number of actions before/after data skipping: {before_count} / {}",
        after.num_rows()
    );
    Ok(after)
}

fn hack_parse(data_fields: &Vec<Field>, stats_schema: &Schema, json_string: Option<&str>) -> DeltaResult<RecordBatch> {
    match json_string {
        Some(s) => Ok(ReaderBuilder::new(stats_schema.clone().into())
            .build(BufReader::new(s.as_bytes()))?
            .collect::<Vec<_>>()
            .into_iter()
            .next()
            .transpose()?
            .ok_or(Error::MissingData("Expected data".into()))?),
        None => Ok(RecordBatch::try_new(
            stats_schema.clone().into(),
            vec![
                Arc::new(new_null_array(
                    &DataType::Struct(data_fields.clone().into()),
                    1,
                )),
                Arc::new(new_null_array(&DataType::Struct(data_fields.clone().into()), 1)),
            ],
        )?),
    }
}
