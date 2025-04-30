use std::fmt::Display;

use wit_encoder::{Interface, Render, RenderOpts};

pub struct WitDisplay<T: Render>(pub T);

impl<T: Render> Display for WitDisplay<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.render(f, &RenderOpts::default())?;

        Ok(())
    }
}

pub struct WitInterfaceDisplay(pub Interface);

impl Display for WitInterfaceDisplay {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let opts = RenderOpts::default();
        write!(f, "{}interface {} {{", opts.spaces(), self.0.name())?;
        if !self.0.uses().is_empty() || !self.0.items().is_empty() {
            writeln!(f)?;
            self.0.uses().to_vec().render(f, &opts.indent())?;
            self.0.items().to_vec().render(f, &opts.indent())?;
            writeln!(f, "{}}}", opts.spaces())?;
        } else {
            writeln!(f, "}}")?;
        }

        Ok(())
    }
}
