use work::blendfiles::Blendfile;

/// A Enum Variant that encodes the varios states a Blendfile can be in.
#[derive(Debug, Clone)]
pub enum Blend{
    Optimized(Blendfile),
    Downloaded(Blendfile),
    None
}


impl Default for Blend {
    fn default() -> Self {
        Blend::None
    }
}

impl Blend{
    pub fn new() -> Self{
        Blend::default()
    }

    pub fn is_none(&self) -> bool{
        match self{
            Blend::None => true,
            _ => false
        }
    }

    pub fn is_some(&self) -> bool{
        match self{
            Blend::None => false,
            _ => true
        }
    }

    pub fn is_downloaded(&self) -> bool{
        match self{
            Blend::None => false,
            _ => true
        }
    }

    pub fn is_optimized(&self) -> bool{
        match self{
            Blend::Optimized(_) => true,
            _ => false
        }
    }

    pub fn unwrap(self) -> Blendfile{
        match self{
            Blend::Downloaded(b) => b,
            Blend::Optimized(b) => b,
            Blend::None => panic!("Called `Blend::unwrap()` on a `Blend::None` value")
        }
    }
}