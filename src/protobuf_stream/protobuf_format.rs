use crate::stream_body_as::StreamBodyAs;
use crate::stream_format::StreamingFormat;
use futures::Stream;
use futures_util::stream::BoxStream;
use futures_util::StreamExt;
use http::HeaderMap;
pub struct ProtobufStreamFormat;

impl ProtobufStreamFormat {
    pub fn new() -> Self {
        Self {}
    }
}

impl<T> StreamingFormat<T> for ProtobufStreamFormat
where
    T: prost::Message + Send + Sync + 'static,
{
    fn to_bytes_stream<'a, 'b>(
        &'a self,
        stream: BoxStream<'b, T>,
    ) -> BoxStream<'b, Result<axum::body::Bytes, axum::Error>> {
        /*fn write_protobuf_record<T: prost::Message>(obj: T) -> Result<Vec<u8>, axum::Error> {
            let v = Vec::new();
            v.extend_from_slice(&(obj.len() as u32).to_be_bytes());
            v.extend(obj);
            println!("v: {:?}", &v);
            Ok(v)
        }*/

        fn write_protobuf_record<T>(obj: T) -> Result<Vec<u8>, axum::Error>
        where
            T: prost::Message,
        {
            let obj_vec = obj.encode_to_vec();
            let mut frame_vec = Vec::new();

            let obj_len: u32 = obj_vec.len() as u32;
            frame_vec.extend_from_slice(&obj_len.to_be_bytes());
            /*
            prost::encoding::encode_varint(obj_len, &mut frame_vec);
            */

            frame_vec.extend(obj_vec);
            println!("frame_vec: {:?}", &frame_vec);
            Ok(frame_vec)
        }

        let stream_bytes: BoxStream<Result<axum::body::Bytes, axum::Error>> = Box::pin({
            stream.map(move |obj| {
                let write_protobuf_res = write_protobuf_record(obj);
                write_protobuf_res.map(axum::body::Bytes::from)
            })
        });

        Box::pin(stream_bytes)
    }

    fn http_response_trailers(&self) -> Option<HeaderMap> {
        let mut header_map = HeaderMap::new();
        header_map.insert(
            http::header::CONTENT_TYPE,
            http::header::HeaderValue::from_static("application/x-protobuf-stream"),
        );
        Some(header_map)
    }
}

impl<'a> StreamBodyAs<'a> {
    pub fn protobuf<S, T>(stream: S) -> Self
    where
        T: prost::Message + Send + Sync + 'static,
        S: Stream<Item = T> + 'a + Send,
    {
        Self::new(ProtobufStreamFormat::new(), stream)
    }
}
