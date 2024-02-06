use std::{io::Cursor, sync::Arc};

use crate::{
    schema::SchemaRef, DeltaResult, EngineData, Error, Expression, FileDataReadResultIterator,
    FileMeta, JsonHandler,
};
use arrow_array::cast::AsArray;
use arrow_json::ReaderBuilder;
use arrow_schema::SchemaRef as ArrowSchemaRef;
use arrow_select::concat::concat_batches;

use super::data::SimpleData;

pub(crate) struct SimpleJsonHandler {}
impl JsonHandler for SimpleJsonHandler {
    fn read_json_files(
        &self,
        files: &[FileMeta],
        schema: SchemaRef,
        _predicate: Option<Expression>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        if files.is_empty() {
            return Ok(Box::new(std::iter::empty()));
        }
        let mut res = vec![];
        for file in files.iter() {
            let d = super::data::SimpleData::try_create_from_json(
                schema.clone(),
                file.location.clone(),
            )?;
            let b: Box<dyn EngineData> = Box::new(d);
            res.push(Ok(b));
        }
        Ok(Box::new(res.into_iter()))
    }

    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        // TODO: This is taken from the default client as it's the same. We should share an
        // implementation at some point
        let raw = Box::into_raw(json_strings) as *mut SimpleData;
        // TODO: Remove unsafe when https://rust-lang.github.io/rfcs/3324-dyn-upcasting.html is
        // stable
        let simple_data = unsafe { Box::from_raw(raw) };
        let json_strings = simple_data.into_record_batch();
        if json_strings.num_columns() != 1 {
            return Err(Error::MissingColumn("Expected single column".into()));
        }
        let json_strings =
            json_strings
                .column(0)
                .as_string_opt::<i32>()
                .ok_or(Error::UnexpectedColumnType(
                    "Expected column to be String".into(),
                ))?;

        let data = json_strings
            .into_iter()
            .filter_map(|d| {
                d.map(|dd| {
                    let mut data = dd.as_bytes().to_vec();
                    data.extend("\n".as_bytes());
                    data
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        let schema: ArrowSchemaRef = Arc::new(output_schema.as_ref().try_into()?);
        let batches = ReaderBuilder::new(schema.clone())
            .build(Cursor::new(data))?
            .collect::<Result<Vec<_>, _>>()?;

        let res: Box<dyn EngineData> =
            Box::new(SimpleData::new(concat_batches(&schema, &batches)?));
        Ok(res)
    }
}