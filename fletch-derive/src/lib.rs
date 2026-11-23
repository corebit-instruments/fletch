use proc_macro::TokenStream;
use quote::quote;
use syn::ext::IdentExt;
use syn::{Data, DeriveInput, Fields, parse_macro_input};

/// Derives `fletch::FletchSchema`: one row-stream column per named field, in
/// field order, typed by the field. `Option<T>` fields write null for `None`.
#[proc_macro_derive(FletchSchema)]
pub fn derive_fletch_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let ident = input.ident;

    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            input.generics,
            "FletchSchema cannot be derived for generic structs",
        )
        .to_compile_error()
        .into();
    }

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return syn::Error::new_spanned(
                    ident,
                    "FletchSchema requires a struct with named fields",
                )
                .to_compile_error()
                .into();
            }
        },
        _ => {
            return syn::Error::new_spanned(ident, "FletchSchema can only be derived for structs")
                .to_compile_error()
                .into();
        }
    };

    let mut columns = Vec::new();
    let mut values = Vec::new();
    for field in fields {
        let name = field.ident.expect("named field");
        let ty = field.ty;
        let column_name = name.unraw().to_string();
        columns.push(quote! {
            let builder = builder
                .column::<<#ty as ::fletch::FletchField>::Column>(#column_name)?;
        });
        values.push(quote! {
            ::fletch::FletchField::row_value(&self.#name)
        });
    }

    quote! {
        impl ::fletch::FletchSchema for #ident {
            fn declare_columns(
                builder: ::fletch::FletchRowStreamBuilder,
            ) -> ::fletch::Result<::fletch::FletchRowStreamBuilder> {
                #(#columns)*
                Ok(builder)
            }

            fn append_to(&self, stream: &mut ::fletch::FletchRowStream) -> ::fletch::Result<()> {
                stream.append(&[#(#values),*])
            }
        }
    }
    .into()
}
