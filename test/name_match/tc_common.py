class TestCase:
    def read_config(self, filename):
        with open(f"{filename}/config.txt", "r") as config_file_obj:
            config_dict = {
                line.split(":")[0]: line.split(":")[1].rstrip("\n")
                for line in config_file_obj.readlines()
            }
        for key, value in config_dict.items():
            setattr(self, key, value)

    def candidate_ids(self):
        return self.initial_candidate_employee_ids.split(",")

    def guesses(self):
        return self.read_file(f"{self.dirname}/guesses.txt")

    def read_file(self, filename):
        with open(filename, "r") as inF:
            return inF.readlines()[0].rstrip("\n")
