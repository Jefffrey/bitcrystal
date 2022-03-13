use urlencoding::decode;

// TODO: implement magnet links

enum ParseError {}

#[derive(Debug)]
struct Magnet {
    display_name: Option<String>,  // dn
    exact_topic: Option<String>,   // xt
    address_trackers: Vec<String>, // tr
}

impl Magnet {
    pub fn new() -> Magnet {
        Magnet {
            display_name: None,
            exact_topic: None,
            address_trackers: vec![],
        }
    }

    pub fn parse(link: &str) -> Result<Magnet, ParseError> {
        let param_string = link
            .strip_prefix("magnet:?")
            .expect("Invalid link: not prefixed with 'magnet:?'");

        let mut magnet = Magnet::new();

        let params = param_string.split("&");
        for q in params {
            let (key, value) = q
                .split_once("=")
                .expect(format!("Invalid query parameter: {}", q).as_str());

            let decoded = decode(value).expect(format!("Cannot url decode: {}", value).as_str());

            match key {
                "dn" => magnet.set_display_name(&decoded),
                "xt" => magnet.set_exact_topic(&decoded),
                "tr" => magnet.add_address_tracker(&decoded),
                _ => panic!("Invalid key: {}", key),
            };
        }

        Ok(magnet)
    }

    pub fn set_display_name(&mut self, display_name: &str) {
        self.display_name = Some(display_name.to_string());
    }

    pub fn set_exact_topic(&mut self, exact_topic: &str) {
        self.exact_topic = Some(exact_topic.to_string());
    }

    pub fn add_address_tracker(&mut self, address_tracker: &str) {
        self.address_trackers.push(address_tracker.to_string());
    }
}
