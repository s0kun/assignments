import os

def find_files(root_dir, suffix):
    matches = []
    for root, _, files in os.walk(root_dir):

        for f in files:
            if f[-len(suffix):] == suffix:
                full_path = os.path.join(root, f)
                matches.append(full_path)

    return matches


if __name__ == "__main__":
    root_dir = "/Project - Healthcare"

    matching_files = find_files(root_dir, ".csv")

    stage = "load"
    # with open("puts.txt","w") as f:
    #     for path in matching_files:
    #         f.write(f"PUT 'file://{path}' @{stage};\n")


# Output:
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Keep/Keep.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Contain/Contain.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Pharmacy/Pharmacy.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Disease/Disease.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Person/Person.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Prescription/Prescription.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/InsuranceCompany/InsuranceCompany.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Patient/Patient.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/InsurancePlan/InsurancePlan.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Address/Address.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Medicine/Medicine.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Treatment/Treatment.csv' @load;
# PUT 'file:///Users/sounak/Documents/assignments/DS - SNOWFLAKE Project - Healthcare/HealthcareTables/Claim/Claim.csv' @load;
