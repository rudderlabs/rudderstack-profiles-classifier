from typing import Any, List, Tuple, Union

import utils
import constants
from logger import logger
from processor import processor

class localProcessor(processor):
    def prepare_feature_table(self, feature_table_name: str, 
                            label_table_name: str,
                            cardinal_feature_threshold: float) -> tuple:
        """This function creates a feature table as per the requirement of customer that is further used for training and prediction.

        Args:
            feature_table_name (str): feature table from the retrieved material_names tuple
            label_table_name (str): label table from the retrieved material_names tuple
        Returns:
            snowflake.snowpark.Table: feature table made using given instance from material names
        """
        try:
            label_ts_col = f"{self.trainer.index_timestamp}_label_ts"
            if self.trainer.eligible_users:
                feature_table = self.connector.get_table_as_dataframe(self.session, feature_table_name, filter_condition=self.trainer.eligible_users)
            else:
                default_user_shortlisting = f"{self.trainer.label_column} != {self.trainer.label_value}"
                feature_table = self.connector.get_table_as_dataframe(self.session, feature_table_name, filter_condition=default_user_shortlisting) #.withColumn(label_ts_col, F.dateadd("day", F.lit(prediction_horizon_days), F.col(index_timestamp)))
            arraytype_features = self.connector.get_arraytype_features(self.session, feature_table_name)
            ignore_features = utils.merge_lists_to_unique(self.trainer.prep.ignore_features, arraytype_features)
            high_cardinal_features = self.connector.get_high_cardinal_features(feature_table, self.trainer.label_column, self.trainer.entity_column, cardinal_feature_threshold)
            ignore_features = utils.merge_lists_to_unique(ignore_features, high_cardinal_features)
            feature_table = self.connector.drop_cols(feature_table, [self.trainer.label_column])
            timestamp_columns = self.trainer.prep.timestamp_columns
            if len(timestamp_columns) == 0:
                timestamp_columns = self.connector.get_timestamp_columns(self.session, feature_table_name, self.trainer.index_timestamp)
            for col in timestamp_columns:
                feature_table = self.connector.add_days_diff(feature_table, col, col, self.trainer.index_timestamp)
            label_table = self.trainer.prepare_label_table(self.connector, self.session, label_table_name, label_ts_col)
            uppercase_list = lambda names: [name.upper() for name in names]
            lowercase_list = lambda names: [name.lower() for name in names]
            ignore_features_ = [col for col in feature_table.columns if col in uppercase_list(ignore_features) or col in lowercase_list(ignore_features)]
            self.trainer.prep.ignore_features = ignore_features_
            self.trainer.prep.timestamp_columns = timestamp_columns
            feature_table = self.connector.join_feature_table_label_table(feature_table, label_table, self.trainer.entity_column, "inner")
            feature_table = self.connector.drop_cols(feature_table, [label_ts_col])
            feature_table = self.connector.drop_cols(feature_table, ignore_features_)
            return feature_table, arraytype_features, timestamp_columns
        except Exception as e:
            print("Exception occured while preparing feature table. Please check the logs for more details")
            raise e

    def train(self, material_names: List[Tuple[str]]):
        min_sample_for_training = constants.MIN_SAMPLES_FOR_TRAINING
        cardinal_feature_threshold = constants.CARDINAL_FEATURE_THRESOLD

        feature_table = None
        for row in material_names:
            feature_table_name, label_table_name = row
            logger.info(f"Preparing training dataset using {feature_table_name} and {label_table_name} as feature and label tables respectively")
            feature_table_instance, arraytype_features, timestamp_columns = self.prepare_feature_table(feature_table_name, 
                                                                    label_table_name,
                                                                    cardinal_feature_threshold)
            if feature_table is None:
                feature_table = feature_table_instance
                break
            else:
                feature_table = feature_table.unionAllByName(feature_table_instance)
        feature_table_name_remote = f"{self.trainer.output_profiles_ml_model}_features"
        filtered_feature_table = self.connector.filter_feature_table(feature_table, self.trainer.entity_column, 
                                                                     self.trainer.index_timestamp, self.trainer.max_row_count, min_sample_for_training)
        self.connector.write_table(filtered_feature_table, feature_table_name_remote, write_mode="overwrite", if_exists="replace")
        logger.info("Training and fetching the results")

        return arraytype_features, timestamp_columns