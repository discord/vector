use bytes::BytesMut;
use prost::Message;
use std::num::NonZeroUsize;
use tokio_util::codec::Encoder;
use vector_lib::codecs::encoding::ProtobufSerializer;
use vector_lib::event::Finalizable;
use vector_lib::request_metadata::RequestMetadata;

use super::proto::third_party::google::cloud::bigquery::storage::v1 as proto;
use super::service::BigqueryRequest;
use crate::event::{Event, EventFinalizers};
use crate::sinks::util::metadata::RequestMetadataBuilder;
use crate::sinks::util::IncrementalRequestBuilder;

// 10MB maximum message size:
// https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsrequest
pub const MAX_BATCH_PAYLOAD_SIZE: usize = 10_000_000;

#[derive(Debug, snafu::Snafu)]
pub enum BigqueryRequestBuilderError {
    #[snafu(display("Encoding protobuf failed: {}", message))]
    ProtobufEncoding { message: String }, // `error` needs to be some concrete type
}

impl From<vector_lib::Error> for BigqueryRequestBuilderError {
    fn from(error: vector_lib::Error) -> Self {
        BigqueryRequestBuilderError::ProtobufEncoding {
            message: format!("{:?}", error),
        }
    }
}

#[derive(Default)]
pub struct BigqueryRequestMetadata {
    request_metadata: RequestMetadata,
    finalizers: EventFinalizers,
}

pub struct BigqueryRequestBuilder {
    pub protobuf_serializer: ProtobufSerializer,
    pub write_stream: String,
}

impl BigqueryRequestBuilder {
    fn build_proto_data(
        &self,
        serialized_rows: Vec<Vec<u8>>,
    ) -> (NonZeroUsize, proto::append_rows_request::ProtoData) {
        let proto_data = proto::append_rows_request::ProtoData {
            writer_schema: Some(proto::ProtoSchema {
                proto_descriptor: Some(translate_descriptor_proto(
                    self.protobuf_serializer.descriptor_proto().clone(),
                )),
            }),
            rows: Some(proto::ProtoRows { serialized_rows }),
        };
        let size = NonZeroUsize::new(proto_data.encoded_len())
            .expect("encoded payload can never be empty");
        (size, proto_data)
    }
}

impl IncrementalRequestBuilder<Vec<Event>> for BigqueryRequestBuilder {
    type Metadata = BigqueryRequestMetadata;
    type Payload = proto::append_rows_request::ProtoData;
    type Request = BigqueryRequest;
    type Error = BigqueryRequestBuilderError;

    fn encode_events_incremental(
        &mut self,
        input: Vec<Event>,
    ) -> Vec<Result<(Self::Metadata, Self::Payload), Self::Error>> {
        let base_proto_data_size: NonZeroUsize = self.build_proto_data(vec![]).0;
        let max_serialized_rows_len: usize = MAX_BATCH_PAYLOAD_SIZE - base_proto_data_size.get();
        let metadata = RequestMetadataBuilder::from_events(&input);
        let mut errors: Vec<Self::Error> = vec![];
        let mut bodies: Vec<(EventFinalizers, (NonZeroUsize, Self::Payload))> = vec![];
        let mut event_finalizers = EventFinalizers::DEFAULT;
        let mut serialized_rows: Vec<Vec<u8>> = vec![];
        let mut serialized_rows_len: usize = 0;
        for mut event in input.into_iter() {
            let current_event_finalizers = event.take_finalizers();
            let mut bytes = BytesMut::new();
            if let Err(e) = self.protobuf_serializer.encode(event, &mut bytes) {
                errors.push(BigqueryRequestBuilderError::ProtobufEncoding {
                    message: format!("{:?}", e),
                });
            } else {
                if bytes.len() + serialized_rows_len > max_serialized_rows_len {
                    // there's going to be too many events to send in one body;
                    // flush the current events and start a new body
                    bodies.push((event_finalizers, self.build_proto_data(serialized_rows)));
                    event_finalizers = EventFinalizers::DEFAULT;
                    serialized_rows = vec![];
                    serialized_rows_len = 0;
                }
                event_finalizers.merge(current_event_finalizers);
                serialized_rows_len += bytes.len();
                serialized_rows.push(bytes.into());
            }
        }
        // flush the final body (if there are any events left)
        if !serialized_rows.is_empty() {
            bodies.push((event_finalizers, self.build_proto_data(serialized_rows)));
        }
        // throw everything together into the expected IncrementalRequestBuilder return type
        bodies
            .into_iter()
            .map(|(event_finalizers, (size, proto_data))| {
                Ok((
                    BigqueryRequestMetadata {
                        finalizers: event_finalizers,
                        request_metadata: metadata.with_request_size(size),
                    },
                    proto_data,
                ))
            })
            .chain(errors.into_iter().map(Err))
            .collect()
    }

    fn build_request(&mut self, metadata: Self::Metadata, payload: Self::Payload) -> Self::Request {
        let request = proto::AppendRowsRequest {
            write_stream: self.write_stream.clone(),
            offset: None, // not supported by _default stream
            trace_id: Default::default(),
            missing_value_interpretations: Default::default(),
            default_missing_value_interpretation: 0,
            rows: Some(proto::append_rows_request::Rows::ProtoRows(payload)),
        };
        let uncompressed_size = request.encoded_len();
        BigqueryRequest {
            request,
            metadata: metadata.request_metadata,
            finalizers: metadata.finalizers,
            uncompressed_size,
        }
    }
}

/// Convert from `prost_reflect::prost_types::DescriptorProto` to `prost_types::DescriptorProto`
///
/// Someone upgraded `prost_reflect` without upgrading the other prost crates,
/// so the `prost_types` version used by `prost_reflect` is newer than the version used by vector.
///
/// This function discards any `UninterpretedOption`s.
///
/// "Why don't you just upgrade `prost_types` to match the version used by `prost_reflect`?
/// Ha. Hahaha. Hahahahahahaha. My branches are littered with the corpses of such attempts.
fn translate_descriptor_proto(
    old_descriptor: prost_reflect::prost_types::DescriptorProto,
) -> prost_types::DescriptorProto {
    prost_types::DescriptorProto {
        name: old_descriptor.name,
        field: old_descriptor
            .field
            .into_iter()
            .map(|field| prost_types::FieldDescriptorProto {
                name: field.name,
                number: field.number,
                label: field.label,
                r#type: field.r#type,
                type_name: field.type_name,
                extendee: field.extendee,
                default_value: field.default_value,
                oneof_index: field.oneof_index,
                json_name: field.json_name,
                options: field.options.map(|options| prost_types::FieldOptions {
                    ctype: options.ctype,
                    packed: options.packed,
                    jstype: options.jstype,
                    lazy: options.lazy,
                    deprecated: options.deprecated,
                    weak: options.weak,
                    uninterpreted_option: Default::default(),
                }),
                proto3_optional: field.proto3_optional,
            })
            .collect(),
        extension: old_descriptor
            .extension
            .into_iter()
            .map(|field| prost_types::FieldDescriptorProto {
                name: field.name,
                number: field.number,
                label: field.label,
                r#type: field.r#type,
                type_name: field.type_name,
                extendee: field.extendee,
                default_value: field.default_value,
                oneof_index: field.oneof_index,
                json_name: field.json_name,
                options: field.options.map(|options| prost_types::FieldOptions {
                    ctype: options.ctype,
                    packed: options.packed,
                    jstype: options.jstype,
                    lazy: options.lazy,
                    deprecated: options.deprecated,
                    weak: options.weak,
                    uninterpreted_option: Default::default(),
                }),
                proto3_optional: field.proto3_optional,
            })
            .collect(),
        nested_type: old_descriptor
            .nested_type
            .into_iter()
            .map(translate_descriptor_proto)
            .collect(),
        enum_type: old_descriptor
            .enum_type
            .into_iter()
            .map(|enum_descriptor| prost_types::EnumDescriptorProto {
                name: enum_descriptor.name,
                value: enum_descriptor
                    .value
                    .into_iter()
                    .map(|value| prost_types::EnumValueDescriptorProto {
                        name: value.name,
                        number: value.number,
                        options: value.options.map(|options| prost_types::EnumValueOptions {
                            deprecated: options.deprecated,
                            uninterpreted_option: Default::default(),
                        }),
                    })
                    .collect(),
                options: enum_descriptor
                    .options
                    .map(|options| prost_types::EnumOptions {
                        allow_alias: options.allow_alias,
                        deprecated: options.deprecated,
                        uninterpreted_option: Default::default(),
                    }),
                reserved_range: enum_descriptor
                    .reserved_range
                    .into_iter()
                    .map(
                        |reserved_range| prost_types::enum_descriptor_proto::EnumReservedRange {
                            start: reserved_range.start,
                            end: reserved_range.end,
                        },
                    )
                    .collect(),
                reserved_name: enum_descriptor.reserved_name,
            })
            .collect(),
        extension_range: old_descriptor
            .extension_range
            .into_iter()
            .map(
                |extension_range| prost_types::descriptor_proto::ExtensionRange {
                    start: extension_range.start,
                    end: extension_range.end,
                    options: extension_range
                        .options
                        .map(|_| prost_types::ExtensionRangeOptions {
                            uninterpreted_option: Default::default(),
                        }),
                },
            )
            .collect(),
        oneof_decl: old_descriptor
            .oneof_decl
            .into_iter()
            .map(|oneof| prost_types::OneofDescriptorProto {
                name: oneof.name,
                options: oneof.options.map(|_| prost_types::OneofOptions {
                    uninterpreted_option: Default::default(),
                }),
            })
            .collect(),
        options: old_descriptor
            .options
            .map(|options| prost_types::MessageOptions {
                message_set_wire_format: options.message_set_wire_format,
                no_standard_descriptor_accessor: options.no_standard_descriptor_accessor,
                deprecated: options.deprecated,
                map_entry: options.map_entry,
                uninterpreted_option: Default::default(),
            }),
        reserved_range: old_descriptor
            .reserved_range
            .into_iter()
            .map(
                |reserved_range| prost_types::descriptor_proto::ReservedRange {
                    start: reserved_range.start,
                    end: reserved_range.end,
                },
            )
            .collect(),
        reserved_name: old_descriptor.reserved_name,
    }
}

#[cfg(test)]
mod test {
    use bytes::{BufMut, Bytes, BytesMut};
    use codecs::encoding::{ProtobufSerializerConfig, ProtobufSerializerOptions};
    use std::collections::BTreeMap;
    use std::path::PathBuf;
    use vector_lib::event::{Event, EventMetadata, LogEvent, Value};

    use super::BigqueryRequestBuilder;
    use crate::sinks::util::IncrementalRequestBuilder;

    #[test]
    fn encode_events_incremental() {
        // build the request builder
        let desc_file = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap())
            .join("lib/codecs/tests/data/protobuf/test.desc");
        let protobuf_serializer = ProtobufSerializerConfig {
            protobuf: ProtobufSerializerOptions {
                desc_file,
                message_type: "test.Bytes".into(),
            },
        }
        .build()
        .unwrap();
        let mut request_builder = BigqueryRequestBuilder {
            protobuf_serializer,
            write_stream: "/projects/123/datasets/456/tables/789/streams/_default".to_string(),
        };
        // check that we break up large batches to avoid api limits
        let mut events = vec![];
        let mut data = BytesMut::with_capacity(63336);
        for i in 1..data.capacity() {
            data.put_u64(i as u64);
        }
        for _ in 0..128 {
            let event = Event::Log(LogEvent::from_parts(
                Value::Object(BTreeMap::from([
                    ("text".into(), Value::Bytes(Bytes::from("hello world"))),
                    ("binary".into(), Value::Bytes(data.clone().into())),
                ])),
                EventMetadata::default(),
            ));
            events.push(event);
        }
        let results = request_builder.encode_events_incremental(events);
        assert!(results.iter().all(|r| r.is_ok()));
        assert!(results.len() > 1);
        // check that we don't generate bodies with no events in them
        let results = request_builder.encode_events_incremental(vec![]);
        assert!(results.is_empty());
    }
}
