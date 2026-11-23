use proc_macro::TokenStream;
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::ext::IdentExt;
use syn::{Data, DeriveInput, Fields, LitStr, parse_macro_input};

/// Derives `fletch::FletchSchema`: one row-stream column per named field, in
/// field order, typed by the field. `Option<T>` fields write null for `None`.
///
/// It also generates one `pub const <FIELD>: fletch::Column<_>` handle per
/// field, and, with `#[fletch(stream = "Name")]`, `fletch::NamedSchema`.
#[proc_macro_derive(FletchSchema, attributes(fletch))]
pub fn derive_fletch_schema(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

fn expand(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let ident = input.ident;

    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "FletchSchema cannot be derived for generic structs",
        ));
    }

    let mut stream_name: Option<LitStr> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("fletch") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("stream") {
                if stream_name.is_some() {
                    return Err(meta.error("duplicate `stream` attribute"));
                }
                stream_name = Some(meta.value()?.parse()?);
                Ok(())
            } else {
                Err(meta.error("unknown fletch attribute; expected `stream = \"Name\"`"))
            }
        })?;
    }

    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => fields.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    ident,
                    "FletchSchema requires a struct with named fields",
                ));
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                ident,
                "FletchSchema can only be derived for structs",
            ));
        }
    };

    let field_count = fields.len();
    let mut declare = Vec::new();
    let mut values = Vec::new();
    let mut column_types = Vec::new();
    let mut reads = Vec::new();
    let mut handles = Vec::new();
    for (index, field) in fields.into_iter().enumerate() {
        let name = field.ident.expect("named field");
        let ty = field.ty;
        let column_name = name.unraw().to_string();
        let column_ty = quote! { <#ty as ::fletch::FletchField>::Column };
        let handle = format_ident!("{}", column_name.to_uppercase(), span = Span::call_site());

        declare.push(quote! {
            let builder = builder.column::<#column_ty>(#column_name)?;
        });
        values.push(quote! {
            ::fletch::FletchField::row_value(&self.#name)
        });
        column_types.push(quote! {
            (#column_name, <#column_ty as ::fletch::FletchType>::data_type())
        });
        reads.push(quote! {
            #name: ::fletch::FletchField::from_row_value(row[#index], #column_name)?
        });
        let doc = format!("The `{column_name}` column.");
        handles.push(quote! {
            #[doc = #doc]
            pub const #handle: ::fletch::Column<#column_ty> = ::fletch::Column::new(#column_name);
        });
    }

    let named = stream_name.map(|stream_name| {
        quote! {
            impl ::fletch::NamedSchema for #ident {
                const STREAM_NAME: &'static str = #stream_name;
            }
        }
    });

    Ok(quote! {
        impl ::fletch::FletchSchema for #ident {
            fn declare_columns(
                builder: ::fletch::FletchRowStreamBuilder,
            ) -> ::fletch::Result<::fletch::FletchRowStreamBuilder> {
                #(#declare)*
                Ok(builder)
            }

            fn append_to(&self, stream: &mut ::fletch::FletchRowStream) -> ::fletch::Result<()> {
                stream.append(&[#(#values),*])
            }

            fn column_types() -> ::std::vec::Vec<(&'static str, ::fletch::DataType)> {
                ::std::vec![#(#column_types),*]
            }

            fn from_row(row: &[::fletch::RowValue<'_>]) -> ::fletch::Result<Self> {
                ::fletch::check_row_len(row, #field_count)?;
                Ok(Self {
                    #(#reads),*
                })
            }
        }

        impl #ident {
            #(#handles)*
        }

        #named
    })
}

/// Derives `fletch::FletchEnum` for a fieldless enum, stored as a string
/// column holding each variant's name. See `fletch::FletchEnum` for the
/// `rename` and `rename_all` attributes.
#[proc_macro_derive(FletchEnum, attributes(fletch))]
pub fn derive_fletch_enum(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_enum(input) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}

const RENAME_RULES: &[&str] = &[
    "lowercase",
    "UPPERCASE",
    "PascalCase",
    "camelCase",
    "snake_case",
    "SCREAMING_SNAKE_CASE",
    "kebab-case",
    "SCREAMING-KEBAB-CASE",
];

fn expand_enum(input: DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let ident = input.ident;
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            input.generics,
            "FletchEnum cannot be derived for generic enums",
        ));
    }

    let mut rename_all: Option<LitStr> = None;
    for attr in &input.attrs {
        if !attr.path().is_ident("fletch") {
            continue;
        }
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                let rule: LitStr = meta.value()?.parse()?;
                if !RENAME_RULES.contains(&rule.value().as_str()) {
                    return Err(syn::Error::new_spanned(
                        &rule,
                        format!(
                            "unknown rename_all rule; expected one of {}",
                            RENAME_RULES.join(", ")
                        ),
                    ));
                }
                rename_all = Some(rule);
                Ok(())
            } else {
                Err(meta.error("unknown fletch attribute; expected `rename_all = \"...\"`"))
            }
        })?;
    }

    let variants = match input.data {
        Data::Enum(data) => data.variants,
        _ => {
            return Err(syn::Error::new_spanned(
                ident,
                "FletchEnum can only be derived for enums",
            ));
        }
    };
    if variants.is_empty() {
        return Err(syn::Error::new_spanned(
            ident,
            "FletchEnum requires at least one variant",
        ));
    }

    let mut names: Vec<(syn::Ident, String)> = Vec::new();
    for variant in variants {
        if !matches!(variant.fields, Fields::Unit) {
            return Err(syn::Error::new_spanned(
                &variant,
                "FletchEnum variants cannot carry data; each variant is stored as its name",
            ));
        }
        let mut rename: Option<LitStr> = None;
        for attr in &variant.attrs {
            if !attr.path().is_ident("fletch") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("rename") {
                    rename = Some(meta.value()?.parse()?);
                    Ok(())
                } else {
                    Err(meta.error("unknown fletch attribute; expected `rename = \"...\"`"))
                }
            })?;
        }
        let identifier = variant.ident.unraw().to_string();
        let name = match (&rename, &rename_all) {
            (Some(rename), _) => rename.value(),
            (None, Some(rule)) => apply_rename_rule(&identifier, &rule.value()),
            (None, None) => identifier,
        };
        if names.iter().any(|(_, existing)| *existing == name) {
            return Err(syn::Error::new_spanned(
                &variant.ident,
                format!("two variants are both stored as `{name}`"),
            ));
        }
        names.push((variant.ident, name));
    }

    let to_name = names
        .iter()
        .map(|(variant, name)| quote! { Self::#variant => #name });
    let from_name = names
        .iter()
        .map(|(variant, name)| quote! { #name => Some(Self::#variant) });

    Ok(quote! {
        impl ::fletch::FletchEnum for #ident {
            fn name(&self) -> &'static str {
                match self {
                    #(#to_name),*
                }
            }

            fn from_name(name: &str) -> ::std::option::Option<Self> {
                match name {
                    #(#from_name,)*
                    _ => None,
                }
            }
        }

        impl ::fletch::FletchType for #ident {
            fn data_type() -> ::fletch::DataType {
                ::fletch::DataType::Utf8
            }

            fn new_builder(capacity: usize) -> ::fletch::ColumnBuilder {
                <::std::string::String as ::fletch::FletchType>::new_builder(capacity)
            }
        }

        impl ::fletch::FletchField for #ident {
            type Column = #ident;

            fn row_value(&self) -> ::fletch::RowValue<'_> {
                ::fletch::RowValue::Str(::fletch::FletchEnum::name(self))
            }

            fn from_row_value(
                value: ::fletch::RowValue<'_>,
                column: &str,
            ) -> ::fletch::Result<Self> {
                ::fletch::enum_from_row_value(value, column)
            }
        }

        impl ::fletch::ColumnValue<#ident> for #ident {
            fn into_literal(self) -> ::fletch::ColumnLiteral {
                ::fletch::ColumnLiteral::Str(
                    ::std::string::ToString::to_string(::fletch::FletchEnum::name(&self)),
                )
            }
        }

        impl ::std::convert::From<#ident> for ::fletch::RowValue<'static> {
            fn from(value: #ident) -> Self {
                ::fletch::RowValue::Str(::fletch::FletchEnum::name(&value))
            }
        }
    })
}

/// Splits an identifier into words: a word starts after `_`, at an uppercase
/// letter after a lowercase letter or digit, or at the last capital of an
/// acronym followed by a lowercase letter. Digits stay with their word.
fn words(identifier: &str) -> Vec<String> {
    let chars: Vec<char> = identifier.chars().collect();
    let mut words: Vec<String> = vec![String::new()];
    for (i, &c) in chars.iter().enumerate() {
        if c == '_' {
            words.push(String::new());
            continue;
        }
        let starts_word = i > 0
            && c.is_uppercase()
            && (chars[i - 1].is_lowercase()
                || chars[i - 1].is_ascii_digit()
                || (chars[i - 1].is_uppercase()
                    && chars.get(i + 1).is_some_and(|next| next.is_lowercase())));
        if starts_word {
            words.push(String::new());
        }
        words.last_mut().expect("words is never empty").push(c);
    }
    words.retain(|word| !word.is_empty());
    words
}

fn capitalize(word: &str) -> String {
    let mut chars = word.chars();
    match chars.next() {
        Some(first) => first
            .to_uppercase()
            .chain(chars.flat_map(char::to_lowercase))
            .collect(),
        None => String::new(),
    }
}

fn apply_rename_rule(identifier: &str, rule: &str) -> String {
    let words = words(identifier);
    let lower = words.iter().map(|w| w.to_lowercase()).collect::<Vec<_>>();
    let upper = words.iter().map(|w| w.to_uppercase()).collect::<Vec<_>>();
    match rule {
        "lowercase" => lower.concat(),
        "UPPERCASE" => upper.concat(),
        "PascalCase" => words.iter().map(|w| capitalize(w)).collect(),
        "camelCase" => words
            .iter()
            .enumerate()
            .map(|(i, w)| {
                if i == 0 {
                    w.to_lowercase()
                } else {
                    capitalize(w)
                }
            })
            .collect(),
        "snake_case" => lower.join("_"),
        "SCREAMING_SNAKE_CASE" => upper.join("_"),
        "kebab-case" => lower.join("-"),
        "SCREAMING-KEBAB-CASE" => upper.join("-"),
        _ => unreachable!("rename rules are checked when parsed"),
    }
}
