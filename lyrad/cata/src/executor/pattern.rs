#[derive(Clone, Copy)]
enum LikeToken {
    Any,
    One,
    Literal(char),
}

pub(crate) fn matches_like(value: &str, pattern: &str) -> bool {
    let mut tokens = Vec::new();
    let mut chars = pattern.chars();
    while let Some(character) = chars.next() {
        tokens.push(match character {
            '%' => LikeToken::Any,
            '_' => LikeToken::One,
            '\\' => LikeToken::Literal(chars.next().unwrap_or('\\')),
            character => LikeToken::Literal(character),
        });
    }

    let value = value.chars().collect::<Vec<_>>();
    let mut matched = vec![false; value.len() + 1];
    matched[0] = true;
    for token in tokens {
        let mut next = vec![false; value.len() + 1];
        if matches!(token, LikeToken::Any) {
            next[0] = matched[0];
        }
        for index in 1..=value.len() {
            next[index] = match token {
                LikeToken::Any => next[index - 1] || matched[index],
                LikeToken::One => matched[index - 1],
                LikeToken::Literal(character) => {
                    matched[index - 1] && value[index - 1] == character
                }
            };
        }
        matched = next;
    }
    matched[value.len()]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_sql_like_patterns() {
        assert!(matches_like("kafka_password", "kafka%"));
        assert!(matches_like("secret1", "secret_"));
        assert!(matches_like("literal_percent%", r"literal\_percent\%"));
        assert!(!matches_like("Kafka_password", "kafka%"));
    }
}
