import json

def reconstruct_strings(token_data: list) -> list:
    """
    Reconstructs possible strings from token data by recursively appending tokens based on their positions.

    Args:
        token_data (list): A list containing dictionaries of token information.

    Returns:
        list: A list of reconstructed strings.
    """
    # Extract the list of dictionaries (tokens) from the last item in token_data
    tokens = list(token_data[-1].values())[0]

    def generate_strings(current_position: int, current_string: str) -> list:
        """
        Recursively generates possible strings starting from the current position.

        Args:
            current_position (int): The current token position to start from.
            current_string (str): The string constructed so far.

        Returns:
            list: A list of strings generated from the current position.
        """
        # Find all tokens starting at the current position
        possible_tokens = [token for token in tokens if token['position'] == current_position]

        if not possible_tokens:
            # If no tokens are found, return the current string as a complete string
            return [current_string.strip()]

        results = []
        for token in possible_tokens:
            # Append the current token's text to the current string
            new_string = current_string + token['text'] + ' '

            # Calculate the next position to move to
            next_position = current_position + token['org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute#positionLength']

            # Recursively generate the rest of the strings
            results.extend(generate_strings(next_position, new_string))

        return results

    # Start generating strings from position 1 with an empty string
    return generate_strings(1, '')




            
