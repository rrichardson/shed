extern crate proc_macro;

use syn;
use quote::quote;
use proc_macro2;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use proc_macro2::{
    Ident,
};
use syn::{
    DataEnum,
    DeriveInput,
    Variant,
    Fields,
    Meta,
    MetaNameValue,
    Lit
};

/* Get the value for the name=value in the attribute if it exists*/
fn find_attr_value<'s>(field: &'s Variant, name: &str) -> Option<String> {
    field.attrs.iter().find_map(|a| a.parse_meta().ok().and_then(|meta|
        if let Meta::NameValue(MetaNameValue{path, lit, ..}) = meta {
            if path.is_ident(name) {
                if let Lit::Str(val) = lit {
                    Some(val.value())
                } else { None }
            } else { None }
        } else { None }
    ))
}


#[proc_macro_derive(Manifold, attributes(prefix))]
pub fn manifold(input: TokenStream) -> TokenStream {
    // Parse the string representation
    let ast: DeriveInput = syn::parse(input).expect("Couldn't parse for getters");
    let mut builder = Builder::new(&ast);
    let gen = builder.run(&ast);
    gen.into()
}

struct Builder {
    variants: Vec<(Variant, String)>,
    typename: Ident,
}

impl Builder {
    pub fn new(ast: &DeriveInput) -> Builder {
        Builder {
            variants: Vec::new(),
            typename: ast.ident.clone(),
        }
    }

    pub fn run(&mut self, ast: &DeriveInput) -> TokenStream2 {
        let typename = &ast.ident;
        let generics = &ast.generics;
        let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

        // Enums only
        if let syn::Data::Enum(DataEnum {
            ref variants,
            ..
        }) = ast.data {
            // let _stock_methods = create_stock(name);
            self.variants = variants.iter()
                .flat_map(|f| find_attr_value(f, "prefix").map(|val| (f.to_owned(), val)))
                .collect();

            let streams = self.gen_stream_members();
            quote! {
                impl #impl_generics Manifold for #typename #ty_generics #where_clause {
                    fn connect(ds: Store) -> ::futures::stream::SelectAll<Pin<Box<dyn ::futures::Stream<Item = std::result::Result<Self, anyhow::Error>> + Send>>> {
                        ::futures::stream::select_all(vec![
                        #(#streams)*
                        ])
                    }
                }
            }
        } else {
            panic!("#[derive(Manifold)] is only defined for enums, not structs!");
        }
    }

    /* Presently, we only handle single element, unnamed variant enums */
    fn gen_stream_members(&self) -> Vec<TokenStream2> {
        let typename = self.typename.clone();
        self.variants.iter().flat_map(|v| match v.0.fields {
            Fields::Unnamed(ref un) => {
                un.unnamed.first().map(|_| {
                    //let var_ty = f.ty.clone();
                    let name = v.0.ident.clone();
                    let prefix = v.1.clone();
                    quote! {
                        Pipe::from_source(Source(ds.0.watch_prefix(#prefix)), #prefix, None).map(|res| res.map(|(k, v)| #typename::#name((k, v)) )).boxed(),
                    }
                })
            },
            _ => None
        }).collect()
    }
}

/*
    fn connect(&self, tree: Tree) -> stream::SelectAll<Pin<Box<dyn Stream<Item = Result<(Vec<u8>, TestManifold), anyhow::Error>> + Send>>> {
        stream::select_all(
            vec![Pipe::from_sub(tree.watch_prefix(b"foo/"), b"foo/").map(|res| res.map(|(k, v)| (k, TestManifold::Test1(v)))).boxed(),
                 Pipe::from_sub(tree.watch_prefix(b"bar/"), b"bar/").map(|res| res.map(|(k, v)| (k, TestManifold::Test2(v)))).boxed(),
                 Pipe::from_sub(tree.watch_prefix(b"baz/"), b"baz/").map(|res| res.map(|(k, v)| (k, TestManifold::Test3(v)))).boxed()])
    }
 */
