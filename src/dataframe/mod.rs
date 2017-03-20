use super::column::Column;

pub struct DataFrame<C> where C: Column {
    column_name: Vec<String>, //keep name and index synced?
    columns: Vec<Box<C>>,
}
